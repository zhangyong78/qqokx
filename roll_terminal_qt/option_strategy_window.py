from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import traceback
from typing import Any

from PySide6.QtCharts import (
    QCandlestickSeries,
    QCandlestickSet,
    QChart,
    QChartView,
    QDateTimeAxis,
    QLineSeries,
    QValueAxis,
)
from PySide6.QtCore import QDateTime, QObject, QPointF, QRectF, Qt, QThread, QTimer, Signal, Slot
from PySide6.QtGui import QColor, QPainter, QPainterPath, QPen, QPolygonF
from PySide6.QtWidgets import (
    QAbstractItemView,
    QButtonGroup,
    QCheckBox,
    QComboBox,
    QDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QRadioButton,
    QSlider,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from roll_terminal_qt.app_icon import apply_qt_window_icon
from okx_quant.models import Candle, Credentials, Instrument
from okx_quant.okx_client import OkxPosition, OkxRestClient, OkxTicker
from okx_quant.option_strategy import (
    MAX_SIMULATION_VOLATILITY,
    MIN_SIMULATION_VOLATILITY,
    OptionChainRow,
    OptionQuote,
    ResolvedStrategyLeg,
    StrategyLegDefinition,
    StrategyPayoffSnapshot,
    build_composite_candles,
    build_default_formula,
    build_option_chain_rows,
    build_option_pnl_candles,
    build_option_pnl_value,
    build_payoff_snapshot,
    build_simulated_payoff_snapshot,
    convert_candles_by_reference,
    convert_payoff_snapshot_to_usdt,
    estimate_leg_greeks,
    evaluate_linear_formula,
    format_option_expiry_label,
    infer_implied_volatility_for_leg,
    option_contract_value,
    parse_linear_formula,
    parse_option_contract,
    parse_option_expiry_datetime,
    resolve_strategy_leg,
)
from okx_quant.persistence import load_option_strategies_snapshot, save_option_strategies_snapshot
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.option_strategy_ui import (
    BAR_OPTIONS,
    DEFAULT_OPTION_FAMILY_OPTIONS,
    MAX_OPTION_COMBO_CANDLES,
    _align_overlay_three_series,
    _axis_values,
    _build_option_quote,
    _filter_option_positions,
    _format_axis_value,
    _format_compact_number,
    _format_price,
    _format_signed_percent,
    _index_markers,
    _load_deribit_option_chart_candles,
    _native_display_currency,
    _position_side_and_quantity,
    _spot_usdt_inst_id,
    _strategy_leg_quote_currency,
)
from roll_terminal_qt.runtime import load_runtime


_SHARED_CLIENT: OkxRestClient | None = None


def _shared_client() -> OkxRestClient:
    global _SHARED_CLIENT
    if _SHARED_CLIENT is None:
        _SHARED_CLIENT = OkxRestClient()
    return _SHARED_CLIENT


@dataclass(frozen=True)
class ChainSnapshot:
    family: str
    expiry: str
    expiries: tuple[str, ...]
    chain_rows: tuple[OptionChainRow, ...]
    quotes: tuple[OptionQuote, ...]
    family_instruments: tuple[Instrument, ...]
    tickers_by_inst_id: dict[str, OkxTicker]
    underlying_price: Decimal | None


@dataclass(frozen=True)
class ImportSnapshot:
    family: str
    expiry: str
    scope: str
    replace_existing: bool
    imported: tuple[tuple[StrategyLegDefinition, Instrument, OptionQuote | None], ...]
    family_instruments: tuple[Instrument, ...]
    tickers_by_inst_id: dict[str, OkxTicker]


@dataclass(frozen=True)
class ChartSnapshot:
    combo_candles: tuple[Candle, ...]
    requested_limit: int
    source_counts: dict[str, int]
    payoff_snapshot: StrategyPayoffSnapshot | None
    latest_quotes: dict[str, OptionQuote]
    latest_combo_value: Decimal
    spot_usdt_price: Decimal | None
    spot_usdt_candles: tuple[Candle, ...]
    formula: str
    current_underlying_price: Decimal | None
    resolved_legs: tuple[ResolvedStrategyLeg, ...]
    implied_volatility_by_alias: dict[str, Decimal]
    payoff_loaded_at: datetime | None


@dataclass(frozen=True)
class OverlaySnapshot:
    triples: tuple[tuple[Candle, Candle, Candle], ...]
    combo_ccy: str
    spot_inst_id: str
    vol_currency: str
    resolution_label: str
    resolution_note: str


class _OptionChainThread(QThread):
    snapshot_ready = Signal(int, object)
    error_raised = Signal(int, str)

    def __init__(self, *, request_id: int, family: str, preferred_expiry: str, client: OkxRestClient, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._request_id = request_id
        self._family = family
        self._preferred_expiry = preferred_expiry
        self._client = client

    def run(self) -> None:
        try:
            family_instruments = self._fetch_family_instruments_remote(self._family)
            tickers = self._fetch_family_tickers_remote(self._family)
            tickers_by_inst_id = {item.inst_id: item for item in tickers}
            expiries = sorted({parse_option_contract(item.inst_id).expiry_code for item in family_instruments})
            selected_expiry = self._preferred_expiry if self._preferred_expiry in expiries else (expiries[0] if expiries else "")
            selected_instruments = [
                item for item in family_instruments if parse_option_contract(item.inst_id).expiry_code == selected_expiry
            ]
            quotes = tuple(_build_option_quote(item, tickers_by_inst_id.get(item.inst_id)) for item in selected_instruments)
            underlying_price = next((item.index_price for item in quotes if item.index_price is not None), None)
            snapshot = ChainSnapshot(
                family=self._family,
                expiry=selected_expiry,
                expiries=tuple(expiries),
                chain_rows=tuple(build_option_chain_rows(list(quotes))),
                quotes=quotes,
                family_instruments=tuple(family_instruments),
                tickers_by_inst_id=tickers_by_inst_id,
                underlying_price=underlying_price,
            )
            self.snapshot_ready.emit(self._request_id, snapshot)
        except Exception as exc:  # noqa: BLE001
            self.error_raised.emit(self._request_id, str(exc))

    def _fetch_family_instruments_remote(self, family: str) -> list[Instrument]:
        normalized = family.strip().upper()
        raw: list[Instrument] = []
        try:
            raw = self._client.get_option_instruments(inst_family=normalized)
        except Exception:
            raw = []
        if not raw:
            raw = self._client.get_instruments("OPTION", uly=normalized)
        return [item for item in raw if (item.inst_family or "").strip().upper() == normalized]

    def _fetch_family_tickers_remote(self, family: str) -> list[OkxTicker]:
        normalized = family.strip().upper()
        raw: list[OkxTicker] = []
        try:
            raw = self._client.get_tickers("OPTION", inst_family=normalized)
        except Exception:
            raw = []
        if not raw:
            raw = self._client.get_tickers("OPTION", uly=normalized)
        result: list[OkxTicker] = []
        for ticker in raw:
            try:
                if parse_option_contract(ticker.inst_id).inst_family == normalized:
                    result.append(ticker)
            except ValueError:
                continue
        return result


class _LegQuoteThread(QThread):
    snapshot_ready = Signal(object)
    error_raised = Signal(str)

    def __init__(self, *, legs: list[StrategyLegDefinition], instrument_map: dict[str, Instrument], client: OkxRestClient, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._legs = [StrategyLegDefinition(**leg.__dict__) for leg in legs]
        self._instrument_map = dict(instrument_map)
        self._client = client

    def run(self) -> None:
        try:
            refreshed: list[tuple[str, Instrument, OptionQuote]] = []
            for leg in self._legs:
                instrument = self._instrument_map.get(leg.inst_id) or self._client.get_instrument(leg.inst_id)
                ticker = self._client.get_ticker(leg.inst_id)
                refreshed.append((leg.inst_id, instrument, _build_option_quote(instrument, ticker)))
            self.snapshot_ready.emit(tuple(refreshed))
        except Exception as exc:  # noqa: BLE001
            self.error_raised.emit(str(exc))


class _ImportPositionsThread(QThread):
    snapshot_ready = Signal(int, object)
    error_raised = Signal(int, str)

    def __init__(
        self,
        *,
        request_id: int,
        profile_name: str | None,
        family: str,
        expiry: str,
        scope: str,
        replace_existing: bool,
        alias_start: int,
        client: OkxRestClient,
        parent: QObject | None = None,
    ) -> None:
        super().__init__(parent)
        self._request_id = request_id
        self._profile_name = str(profile_name or "").strip()
        self._family = family
        self._expiry = expiry
        self._scope = scope
        self._replace_existing = replace_existing
        self._alias_start = alias_start
        self._client = client

    def run(self) -> None:
        try:
            runtime = load_runtime(self._profile_name) if self._profile_name else (load_runtime("159") or load_runtime())
            if runtime is None:
                raise ValueError("当前未配置可用运行环境，无法导入账户持仓。")
            positions = self._client.get_positions(runtime.credentials, environment=runtime.environment, inst_type="OPTION")
            filtered_positions = _filter_option_positions(
                positions,
                family=self._family,
                expiry_code=self._expiry if self._scope == "expiry" else None,
            )
            if not filtered_positions:
                scope_text = f"{self._family} {self._expiry}" if self._scope == "expiry" and self._expiry else self._family
                raise ValueError(f"当前账户没有可导入的 {scope_text} 期权持仓。")

            family_instruments = self._client.get_option_instruments(inst_family=self._family)
            if not family_instruments:
                family_instruments = self._client.get_instruments("OPTION", uly=self._family)
            family_instruments = [item for item in family_instruments if (item.inst_family or "").strip().upper() == self._family]
            tickers = self._client.get_tickers("OPTION", inst_family=self._family)
            tickers_by_inst_id = {item.inst_id: item for item in tickers}
            instrument_lookup = {item.inst_id: item for item in family_instruments}
            next_alias = self._alias_start
            imports: list[tuple[StrategyLegDefinition, Instrument, OptionQuote | None]] = []
            for position in filtered_positions:
                instrument = instrument_lookup.get(position.inst_id) or self._client.get_instrument(position.inst_id)
                ticker = tickers_by_inst_id.get(position.inst_id)
                quote = _build_option_quote(instrument, ticker) if ticker is not None else None
                next_alias += 1
                side, quantity = _position_side_and_quantity(position)
                premium = position.avg_price if position.avg_price is not None else (quote.reference_price if quote is not None else None)
                imports.append(
                    (
                        StrategyLegDefinition(
                            alias=f"L{next_alias}",
                            inst_id=position.inst_id,
                            side="buy" if side == "buy" else "sell",
                            quantity=quantity,
                            premium=premium,
                            enabled=True,
                        ),
                        instrument,
                        quote,
                    )
                )
            snapshot = ImportSnapshot(
                family=self._family,
                expiry=self._expiry,
                scope=self._scope,
                replace_existing=self._replace_existing,
                imported=tuple(imports),
                family_instruments=tuple(family_instruments),
                tickers_by_inst_id=tickers_by_inst_id,
            )
            self.snapshot_ready.emit(self._request_id, snapshot)
        except Exception as exc:  # noqa: BLE001
            self.error_raised.emit(self._request_id, str(exc))


class _ChartThread(QThread):
    snapshot_ready = Signal(int, object)
    error_raised = Signal(int, str)

    def __init__(
        self,
        *,
        request_id: int,
        mode: str,
        legs: list[StrategyLegDefinition],
        instrument_map: dict[str, Instrument],
        client: OkxRestClient,
        bar: str,
        candle_limit: int,
        formula: str,
        current_underlying_price: Decimal | None,
        parent: QObject | None = None,
    ) -> None:
        super().__init__(parent)
        self._request_id = request_id
        self._mode = mode
        self._legs = [StrategyLegDefinition(**leg.__dict__) for leg in legs]
        self._instrument_map = dict(instrument_map)
        self._client = client
        self._bar = bar
        self._candle_limit = candle_limit
        self._formula = formula
        self._current_underlying_price = current_underlying_price

    def run(self) -> None:
        try:
            active_legs = [item for item in self._legs if item.enabled]
            if not active_legs:
                raise ValueError("请先启用至少一条策略腿。")
            latest_quotes: dict[str, OptionQuote] = {}
            resolved_legs: list[ResolvedStrategyLeg] = []
            candles_by_alias: dict[str, list[Candle]] = {}
            current_underlying_price = self._current_underlying_price
            payoff_loaded_at = datetime.now()
            source_counts: dict[str, int] = {}

            for leg in active_legs:
                instrument = self._instrument_map.get(leg.inst_id) or self._client.get_instrument(leg.inst_id)
                ticker = self._client.get_ticker(leg.inst_id)
                quote = _build_option_quote(instrument, ticker)
                latest_quotes[leg.inst_id] = quote
                if quote.reference_price is None:
                    raise ValueError(f"{leg.inst_id} 当前缺少标记价 / 最新价，无法计算。")
                if leg.premium is None:
                    raise ValueError(f"{leg.inst_id} 缺少持仓价，无法生成组合浮盈亏 K 线。")
                if current_underlying_price is None and quote.index_price is not None:
                    current_underlying_price = quote.index_price
                resolved_legs.append(resolve_strategy_leg(leg, instrument))
                confirmed_candles = [
                    item
                    for item in self._client.get_mark_price_candles(leg.inst_id, self._bar, limit=self._candle_limit)
                    if item.confirmed
                ]
                source_counts[leg.alias] = len(confirmed_candles)
                candles_by_alias[leg.alias] = build_option_pnl_candles(
                    confirmed_candles,
                    entry_price=leg.premium,
                    contract_value=option_contract_value(instrument),
                )

            spot_usdt_price, spot_usdt_candles = self._load_usdt_reference_context(active_legs)
            if current_underlying_price is None and spot_usdt_price is not None:
                current_underlying_price = spot_usdt_price

            latest_values = {
                leg.alias: build_option_pnl_value(
                    latest_quotes[leg.inst_id].reference_price or Decimal("0"),
                    entry_price=leg.premium or Decimal("0"),
                    contract_value=option_contract_value(self._instrument_map.get(leg.inst_id) or latest_quotes[leg.inst_id].instrument),
                )
                for leg in active_legs
            }
            combo_candles = build_composite_candles(
                self._formula,
                candles_by_alias,
                allowed_names=set(latest_values.keys()),
            )
            latest_combo_value = evaluate_linear_formula(
                self._formula,
                latest_values,
                allowed_names=set(latest_values.keys()),
            )

            payoff_snapshot: StrategyPayoffSnapshot | None = None
            implied_volatility_by_alias: dict[str, Decimal] = {}
            if self._mode == "all":
                families = {parse_option_contract(item.inst_id).inst_family for item in active_legs}
                if len(families) != 1:
                    raise ValueError("当前到期盈亏图只支持同一标的系列的期权组合。")
                payoff_snapshot = build_payoff_snapshot(
                    resolved_legs,
                    current_underlying_price=current_underlying_price,
                )
                if current_underlying_price is not None and current_underlying_price > 0:
                    implied_volatility_by_alias = {
                        leg.alias: (
                            infer_implied_volatility_for_leg(
                                leg,
                                settlement_price=current_underlying_price,
                                valuation_time=payoff_loaded_at,
                                option_price=latest_quotes[leg.inst_id].reference_price,
                            )
                            or Decimal("0.6")
                        )
                        for leg in resolved_legs
                    }
            snapshot = ChartSnapshot(
                combo_candles=tuple(combo_candles),
                requested_limit=self._candle_limit,
                source_counts=source_counts,
                payoff_snapshot=payoff_snapshot,
                latest_quotes=latest_quotes,
                latest_combo_value=latest_combo_value,
                spot_usdt_price=spot_usdt_price,
                spot_usdt_candles=tuple(spot_usdt_candles),
                formula=self._formula,
                current_underlying_price=current_underlying_price,
                resolved_legs=tuple(resolved_legs),
                implied_volatility_by_alias=implied_volatility_by_alias,
                payoff_loaded_at=payoff_loaded_at if payoff_snapshot is not None else None,
            )
            self.snapshot_ready.emit(self._request_id, snapshot)
        except Exception as exc:  # noqa: BLE001
            self.error_raised.emit(self._request_id, str(exc))

    def _load_usdt_reference_context(self, active_legs: list[StrategyLegDefinition]) -> tuple[Decimal | None, list[Candle]]:
        families = {parse_option_contract(item.inst_id).inst_family for item in active_legs}
        if len(families) != 1:
            return None, []
        spot_inst_id = _spot_usdt_inst_id(next(iter(families)))
        if not spot_inst_id:
            return None, []
        try:
            spot_ticker = self._client.get_ticker(spot_inst_id)
            spot_price = spot_ticker.last or spot_ticker.bid or spot_ticker.ask
        except Exception:
            spot_price = None
        try:
            spot_candles = [item for item in self._client.get_candles_history(spot_inst_id, self._bar, limit=self._candle_limit) if item.confirmed]
        except Exception:
            spot_candles = []
        return spot_price, spot_candles


class _OverlayThread(QThread):
    snapshot_ready = Signal(int, object)
    error_raised = Signal(int, str)

    def __init__(
        self,
        *,
        request_id: int,
        legs: list[StrategyLegDefinition],
        instrument_map: dict[str, Instrument],
        client: OkxRestClient,
        bar: str,
        candle_limit: int,
        formula: str,
        display_in_usdt: bool,
        parent: QObject | None = None,
    ) -> None:
        super().__init__(parent)
        self._request_id = request_id
        self._legs = [StrategyLegDefinition(**leg.__dict__) for leg in legs]
        self._instrument_map = dict(instrument_map)
        self._client = client
        self._bar = bar
        self._candle_limit = candle_limit
        self._formula = formula
        self._display_in_usdt = display_in_usdt

    def run(self) -> None:
        try:
            active_legs = [item for item in self._legs if item.enabled]
            if not active_legs:
                raise ValueError("请先启用至少一条策略腿。")
            families = {parse_option_contract(item.inst_id).inst_family for item in active_legs}
            if len(families) != 1:
                raise ValueError("叠加对比仅支持同一期权系列的组合。")
            family = next(iter(families))
            spot_inst_id = _spot_usdt_inst_id(family)
            if not spot_inst_id:
                raise ValueError("当前期权系列无法映射到 USDT 现货 K 线。")
            currency = family.split("-", 1)[0]
            candles_by_alias: dict[str, list[Candle]] = {}
            for leg in active_legs:
                instrument = self._instrument_map.get(leg.inst_id) or self._client.get_instrument(leg.inst_id)
                ticker = self._client.get_ticker(leg.inst_id)
                quote = _build_option_quote(instrument, ticker)
                if quote.reference_price is None:
                    raise ValueError(f"{leg.inst_id} 当前缺少标记价 / 最新价。")
                if leg.premium is None:
                    raise ValueError(f"{leg.inst_id} 缺少持仓价。")
                candles = [item for item in self._client.get_mark_price_candles(leg.inst_id, self._bar, limit=self._candle_limit) if item.confirmed]
                candles_by_alias[leg.alias] = build_option_pnl_candles(
                    candles,
                    entry_price=leg.premium,
                    contract_value=option_contract_value(instrument),
                )
            combo_candles = build_composite_candles(
                self._formula,
                candles_by_alias,
                allowed_names={item.alias for item in active_legs},
            )
            spot_candles = [item for item in self._client.get_candles_history(spot_inst_id, self._bar, limit=self._candle_limit) if item.confirmed]
            combo_ccy = _native_display_currency(active_legs, self._instrument_map)
            if self._display_in_usdt:
                converted = convert_candles_by_reference(combo_candles, spot_candles)
                if converted:
                    combo_candles = converted
                    combo_ccy = "USDT"
            vol_candles, resolution_label, resolution_note = _load_deribit_option_chart_candles(
                currency,
                bar=self._bar,
                requested_limit=self._candle_limit,
            )
            triples = _align_overlay_three_series(combo_candles, vol_candles, spot_candles)
            if not triples:
                raise ValueError("组合 K 线、Deribit DVOL 与现货没有可对齐的时间戳。")
            self.snapshot_ready.emit(
                self._request_id,
                OverlaySnapshot(
                    triples=tuple(triples),
                    combo_ccy=combo_ccy,
                    spot_inst_id=spot_inst_id,
                    vol_currency=currency,
                    resolution_label=resolution_label,
                    resolution_note=resolution_note,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            self.error_raised.emit(self._request_id, str(exc))


class PayoffChartView(QWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._message = "加入策略腿后，可生成到期盈亏图。"
        self._snapshot: StrategyPayoffSnapshot | None = None
        self._reference_snapshot: StrategyPayoffSnapshot | None = None
        self._value_ccy = ""
        self._mode_label = "到期盈亏"
        self.setMinimumHeight(320)
        self.setMouseTracking(True)
        self._hover_pos: QPointF | None = None

    def show_message(self, message: str) -> None:
        self._message = message
        self._snapshot = None
        self._reference_snapshot = None
        self._hover_pos = None
        self.update()

    def set_snapshot(
        self,
        snapshot: StrategyPayoffSnapshot,
        *,
        value_ccy: str,
        mode_label: str,
        reference_snapshot: StrategyPayoffSnapshot | None = None,
    ) -> None:
        if not snapshot.points:
            self.show_message("暂无到期盈亏数据。")
            return
        self._message = ""
        self._snapshot = snapshot
        self._reference_snapshot = reference_snapshot if reference_snapshot is not None and reference_snapshot.points else None
        self._value_ccy = value_ccy
        self._mode_label = mode_label
        self._hover_pos = None
        self.update()

    def mouseMoveEvent(self, event) -> None:  # type: ignore[override]
        self._hover_pos = QPointF(event.position())
        self.update()
        super().mouseMoveEvent(event)

    def leaveEvent(self, event) -> None:  # type: ignore[override]
        self._hover_pos = None
        self.update()
        super().leaveEvent(event)

    def paintEvent(self, event) -> None:  # type: ignore[override]
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.fillRect(self.rect(), QColor("#ffffff"))
        try:
            snapshot = self._snapshot
            if snapshot is None or not snapshot.points:
                painter.setPen(QColor("#6e7781"))
                painter.drawText(self.rect(), Qt.AlignmentFlag.AlignCenter, self._message or "暂无到期盈亏数据。")
                return

            reference_snapshot = self._reference_snapshot
            points = sorted(snapshot.points, key=lambda item: item.underlying_price)
            reference_points = sorted(reference_snapshot.points, key=lambda item: item.underlying_price) if reference_snapshot is not None else []
            show_reference = bool(reference_points) and self._mode_label != "到期盈亏"

            width = max(self.width(), 960)
            height = max(self.height(), 420)
            left = 66.0
            right = 24.0
            top = 22.0
            bottom = 40.0
            inner_width = width - left - right
            inner_height = height - top - bottom
            if inner_width <= 0 or inner_height <= 0:
                return

            all_points = points + reference_points
            pnl_values = [item.pnl for item in all_points]
            min_pnl = min(pnl_values)
            max_pnl = max(pnl_values)
            if min_pnl == max_pnl:
                min_pnl -= Decimal("1")
                max_pnl += Decimal("1")
            if min_pnl > 0:
                min_pnl = Decimal("0")
            if max_pnl < 0:
                max_pnl = Decimal("0")

            price_min = min(item.underlying_price for item in all_points)
            price_max = max(item.underlying_price for item in all_points)
            if price_min == price_max:
                price_min -= Decimal("1")
                price_max += Decimal("1")

            def x_for(price: Decimal) -> float:
                ratio = (price - price_min) / max(price_max - price_min, Decimal("0.00000001"))
                return left + float(ratio) * inner_width

            def y_for(pnl: Decimal) -> float:
                ratio = (max_pnl - pnl) / max(max_pnl - min_pnl, Decimal("0.00000001"))
                return top + float(ratio) * inner_height

            zero_y = y_for(Decimal("0"))

            painter.setPen(QPen(QColor("#d0d7de"), 1))
            painter.drawRect(QRectF(left, top, inner_width, inner_height))

            zero_pen = QPen(QColor("#8c959f"), 1)
            zero_pen.setStyle(Qt.PenStyle.DashLine)
            painter.setPen(zero_pen)
            painter.drawLine(QPointF(left, zero_y), QPointF(width - right, zero_y))

            axis_font = painter.font()
            axis_font.setPointSize(9)
            painter.setFont(axis_font)
            for value in _axis_values(min_pnl, max_pnl, steps=4):
                y = y_for(value)
                grid_pen = QPen(QColor("#eaeef2"), 1)
                grid_pen.setStyle(Qt.PenStyle.DashLine)
                painter.setPen(grid_pen)
                painter.drawLine(QPointF(left, y), QPointF(width - right, y))
                painter.setPen(QColor("#57606a"))
                painter.drawText(QRectF(0, y - 10, left - 8, 20), Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, _format_axis_value(value))

            self._draw_payoff_fill(painter, points, x_for, y_for, zero_y, left, top, width - right, height - bottom)

            if show_reference:
                ref_pen = QPen(QColor("#8256d0"), 2)
                ref_path = QPainterPath()
                ref_path.moveTo(QPointF(x_for(reference_points[0].underlying_price), y_for(reference_points[0].pnl)))
                for point in reference_points[1:]:
                    ref_path.lineTo(QPointF(x_for(point.underlying_price), y_for(point.pnl)))
                painter.setPen(ref_pen)
                painter.drawPath(ref_path)

            main_pen = QPen(QColor("#0f766e" if show_reference else "#0969da"), 2)
            main_path = QPainterPath()
            main_path.moveTo(QPointF(x_for(points[0].underlying_price), y_for(points[0].pnl)))
            for point in points[1:]:
                main_path.lineTo(QPointF(x_for(point.underlying_price), y_for(point.pnl)))
            painter.setPen(main_pen)
            painter.drawPath(main_path)

            if snapshot.current_underlying_price is not None:
                current_x = x_for(snapshot.current_underlying_price)
                current_pen = QPen(QColor("#bf8700"), 2)
                painter.setPen(current_pen)
                painter.drawLine(QPointF(current_x, top), QPointF(current_x, height - bottom))
                current_font = painter.font()
                current_font.setBold(True)
                painter.setFont(current_font)
                painter.setPen(QColor("#9a6700"))
                painter.drawText(QRectF(current_x + 6, top + 2, 220, 20), Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop, f"当前 {_format_compact_number(snapshot.current_underlying_price)}")
                painter.setFont(axis_font)

            break_even_pen = QPen(QColor("#cf222e"), 1)
            break_even_pen.setStyle(Qt.PenStyle.DashLine)
            for break_even in snapshot.break_even_prices:
                x = x_for(break_even)
                painter.setPen(break_even_pen)
                painter.drawLine(QPointF(x, top), QPointF(x, height - bottom))
                painter.setPen(QPen(QColor("#cf222e"), 2))
                painter.drawLine(QPointF(x, zero_y - 6), QPointF(x, zero_y + 6))

            for index in _index_markers(len(points), target_count=6):
                point = points[index]
                x = x_for(point.underlying_price)
                marker_pen = QPen(QColor("#f3f4f6"), 1)
                marker_pen.setStyle(Qt.PenStyle.DashLine)
                painter.setPen(marker_pen)
                painter.drawLine(QPointF(x, top), QPointF(x, height - bottom))
                painter.setPen(QColor("#57606a"))
                align = Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop
                if index == 0:
                    align = Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop
                elif index == len(points) - 1:
                    align = Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignTop
                label_rect = QRectF(x - 60, height - bottom + 6, 120, 20)
                painter.drawText(label_rect, align, _format_compact_number(point.underlying_price))

            legend_text = f"{self._mode_label} ({self._value_ccy}) | 绿色=盈利 | 红色=亏损 | 红虚线=盈亏平衡点"
            if show_reference:
                legend_text = f"{self._mode_label}/到期盈亏 ({self._value_ccy}) | 蓝线={self._mode_label} | 灰虚线=到期盈亏 | 绿色=盈利 | 红色=亏损"
            legend_font = painter.font()
            legend_font.setBold(True)
            painter.setFont(legend_font)
            painter.setPen(QColor("#57606a"))
            painter.drawText(QRectF(left, top + 2, inner_width, 18), Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignTop, legend_text)

            hover_pos = self._hover_pos
            if hover_pos is None:
                return
            if not (left <= hover_pos.x() <= (width - right) and top <= hover_pos.y() <= (height - bottom)):
                return
            hover_ratio = max(0.0, min(1.0, (hover_pos.x() - left) / max(inner_width, 1.0)))
            hover_price = price_min + (Decimal(str(hover_ratio)) * (price_max - price_min))
            main_hover = self._interpolate_payoff_value(points, hover_price)
            if main_hover is None:
                return
            main_y = y_for(main_hover)
            marker_positions: list[tuple[float, QColor]] = [(main_y, QColor("#0f766e" if show_reference else "#0969da"))]
            tooltip_lines = [
                f"标的 {_format_compact_number(hover_price)}",
                f"{self._mode_label} {_format_compact_number(main_hover)} {self._value_ccy}",
            ]
            tooltip_y = main_y
            if show_reference:
                ref_hover = self._interpolate_payoff_value(reference_points, hover_price)
                if ref_hover is not None:
                    ref_y = y_for(ref_hover)
                    marker_positions.append((ref_y, QColor("#8256d0")))
                    tooltip_lines.append(f"到期盈亏 {_format_compact_number(ref_hover)} {self._value_ccy}")
                    tooltip_y = min(tooltip_y, ref_y)
            self._draw_hover_overlay(
                painter,
                bounds=QRectF(left, top, inner_width, inner_height),
                hover_x=hover_pos.x(),
                hover_y=tooltip_y,
                marker_positions=tuple(marker_positions),
                marker_color=marker_positions[0][1],
                lines=tuple(tooltip_lines),
            )
        except Exception:
            painter.setPen(QColor("#6e7781"))
            painter.drawText(self.rect(), Qt.AlignmentFlag.AlignCenter, self._message or "图表绘制中，请稍候。")
            return

    def _draw_payoff_fill(
        self,
        painter: QPainter,
        points: list,
        x_for,
        y_for,
        zero_y: float,
        chart_left: float,
        chart_top: float,
        chart_right: float,
        chart_bottom: float,
    ) -> None:
        painter.setPen(Qt.PenStyle.NoPen)
        profit_clip = QRectF(chart_left, chart_top, chart_right - chart_left, max(zero_y - chart_top, 0.0))
        loss_clip = QRectF(chart_left, zero_y, chart_right - chart_left, max(chart_bottom - zero_y, 0.0))
        for previous, current in zip(points, points[1:]):
            x1 = x_for(previous.underlying_price)
            y1 = y_for(previous.pnl)
            x2 = x_for(current.underlying_price)
            y2 = y_for(current.pnl)
            polygon = QPolygonF([QPointF(x1, zero_y), QPointF(x1, y1), QPointF(x2, y2), QPointF(x2, zero_y)])
            if min(y1, y2) < zero_y and profit_clip.height() > 0:
                painter.save()
                painter.setClipRect(profit_clip)
                painter.setBrush(QColor("#c6f6d5"))
                painter.drawPolygon(polygon)
                painter.restore()
            if max(y1, y2) > zero_y and loss_clip.height() > 0:
                painter.save()
                painter.setClipRect(loss_clip)
                painter.setBrush(QColor("#fecaca"))
                painter.drawPolygon(polygon)
                painter.restore()

    def _interpolate_payoff_value(self, points: list, target_price: Decimal) -> Decimal | None:
        if not points:
            return None
        if target_price <= points[0].underlying_price:
            return points[0].pnl
        if target_price >= points[-1].underlying_price:
            return points[-1].pnl
        for previous, current in zip(points, points[1:]):
            if previous.underlying_price <= target_price <= current.underlying_price:
                span = current.underlying_price - previous.underlying_price
                if span == 0:
                    return current.pnl
                ratio = (target_price - previous.underlying_price) / span
                return previous.pnl + ((current.pnl - previous.pnl) * ratio)
        return points[-1].pnl

    def _draw_hover_overlay(
        self,
        painter: QPainter,
        *,
        bounds: QRectF,
        hover_x: float,
        hover_y: float,
        marker_positions: tuple[tuple[float, QColor], ...],
        marker_color: QColor,
        lines: tuple[str, ...],
    ) -> None:
        hover_pen = QPen(QColor("#6e7781"), 1)
        hover_pen.setStyle(Qt.PenStyle.DashLine)
        painter.setPen(hover_pen)
        painter.drawLine(QPointF(hover_x, bounds.top()), QPointF(hover_x, bounds.bottom()))
        painter.drawLine(QPointF(bounds.left(), hover_y), QPointF(bounds.right(), hover_y))
        for marker_y, outline_color in marker_positions:
            painter.setPen(QPen(outline_color, 2))
            painter.setBrush(QColor("#ffffff"))
            painter.drawEllipse(QPointF(hover_x, marker_y), 4.0, 4.0)
        self._draw_hover_tooltip(
            painter,
            bounds=bounds,
            anchor=QPointF(hover_x, hover_y),
            marker_color=marker_color,
            lines=lines,
        )

    def _draw_hover_tooltip(
        self,
        painter: QPainter,
        *,
        bounds: QRectF,
        anchor: QPointF,
        marker_color: QColor,
        lines: tuple[str, ...],
    ) -> None:
        if not lines:
            return
        font = painter.font()
        font.setFamily("Consolas")
        font.setPointSize(8)
        font.setBold(True)
        painter.setFont(font)
        metrics = painter.fontMetrics()
        padding_x = 10.0
        padding_y = 8.0
        line_height = metrics.height() + 1
        text_width = max(metrics.horizontalAdvance(line) for line in lines)
        box_width = text_width + (padding_x * 2)
        box_height = (line_height * len(lines)) + (padding_y * 2)
        place_right = anchor.x() <= bounds.center().x()
        place_above = anchor.y() > bounds.center().y()
        box_left = anchor.x() + 18.0 if place_right else anchor.x() - box_width - 18.0
        box_top = anchor.y() - box_height - 18.0 if place_above else anchor.y() + 18.0
        box_left = max(bounds.left() + 8.0, min(box_left, bounds.right() - box_width - 8.0))
        box_top = max(bounds.top() + 8.0, min(box_top, bounds.bottom() - box_height - 8.0))
        box_rect = QRectF(box_left, box_top, box_width, box_height)
        shadow_rect = box_rect.translated(2.0, 2.0)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QColor(15, 23, 42, 55))
        painter.drawRoundedRect(shadow_rect, 7.0, 7.0)
        guide_y = max(box_rect.top() + 10.0, min(anchor.y(), box_rect.bottom() - 10.0))
        guide_x = box_rect.left() if place_right else box_rect.right()
        painter.setPen(QPen(marker_color.lighter(120), 1))
        painter.drawLine(anchor, QPointF(guide_x, guide_y))
        painter.setPen(QPen(marker_color.lighter(115), 1))
        painter.setBrush(QColor(11, 18, 32, 236))
        painter.drawRoundedRect(box_rect, 7.0, 7.0)
        text_top = box_top + padding_y + metrics.ascent()
        for index, line in enumerate(lines):
            painter.setPen(QColor("#cbd5e1") if index == 0 else QColor("#f8fafc"))
            painter.drawText(QPointF(box_left + padding_x, text_top), line)
            text_top += line_height


def _build_moving_average_series(candles: list[Candle]) -> tuple[list[Decimal], list[Decimal | None]]:
    if not candles:
        return [], []
    closes = [candle.close for candle in candles]
    ema_period = 15
    sma_period = 50
    ema_multiplier = Decimal("2") / Decimal(ema_period + 1)
    ema15_values: list[Decimal] = []
    sma50_values: list[Decimal | None] = []
    rolling_sum = Decimal("0")

    for index, close in enumerate(closes):
        ema15_values.append(close if index == 0 else ((close - ema15_values[-1]) * ema_multiplier) + ema15_values[-1])
        rolling_sum += close
        if index >= sma_period:
            rolling_sum -= closes[index - sma_period]
        sma50_values.append(rolling_sum / Decimal(sma_period) if index + 1 >= sma_period else None)
    return ema15_values, sma50_values


class CandlestickChartView(QChartView):
    hover_changed = Signal(int, float)
    hover_cleared = Signal()

    def __init__(self, *, percent_axis: bool = False, parent: QWidget | None = None) -> None:
        chart = QChart()
        chart.legend().hide()
        super().__init__(chart, parent)
        self._percent_axis = percent_axis
        self.setRenderHint(QPainter.RenderHint.Antialiasing, False)
        self.setMouseTracking(True)
        self.viewport().setMouseTracking(True)
        self._candles: list[Candle] = []
        self._moving_average_values: list[tuple[int, float]] = []
        self._tooltip_close_usdt_rate: Decimal | None = None
        self._tooltip_close_usdt_basis = ""
        self._tooltip_entry_price: Decimal | None = None
        self._hover_pos: QPointF | None = None
        self._value_min = 0.0
        self._value_max = 1.0
        self._hide_wicks = False
        self._chart_title = ""
        self._axis_x: QDateTimeAxis | None = None
        self._axis_y: QValueAxis | None = None
        self._full_x_min_ms = 0.0
        self._full_x_max_ms = 1.0
        self._full_y_min = 0.0
        self._full_y_max = 1.0
        self._pan_anchor_x: float | None = None
        self._linked_hover_index: int | None = None
        self._linked_hover_y_ratio: float | None = None
        self._price_badge = self._create_hover_label(multiline=False, center=True)
        self._time_badge = self._create_hover_label(multiline=False, center=True)
        self._tooltip_badge = self._create_hover_label(multiline=True, center=False)
        self._hide_hover_overlays()

    def show_message(self, message: str) -> None:
        self._clear_chart_context()
        chart = self.chart()
        chart.removeAllSeries()
        for axis in chart.axes():
            chart.removeAxis(axis)
        chart.setTitle(message)
        self._chart_title = message

    def _clear_chart_context(self) -> None:
        self._axis_x = None
        self._axis_y = None
        self._candles = []
        self._moving_average_values = []
        self._tooltip_close_usdt_rate = None
        self._tooltip_close_usdt_basis = ""
        self._tooltip_entry_price = None
        self._hover_pos = None
        self._pan_anchor_x = None
        self._linked_hover_index = None
        self._linked_hover_y_ratio = None
        self._hide_hover_overlays()
        self.viewport().update()

    def set_candles(
        self,
        *,
        title: str,
        candles: list[Candle],
        hide_wicks: bool = False,
        show_moving_averages: bool = False,
        tooltip_close_usdt_rate: Decimal | None = None,
        tooltip_close_usdt_basis: str = "",
        tooltip_entry_price: Decimal | None = None,
    ) -> None:
        if not candles:
            self.show_message(title)
            return
        self._hide_wicks = hide_wicks
        self._chart_title = title
        self._clear_chart_context()
        if tooltip_close_usdt_rate is not None and tooltip_close_usdt_rate > 0:
            self._tooltip_close_usdt_rate = tooltip_close_usdt_rate
            self._tooltip_close_usdt_basis = tooltip_close_usdt_basis.strip()
            if tooltip_entry_price is not None and tooltip_entry_price > 0:
                self._tooltip_entry_price = tooltip_entry_price
        chart = self.chart()
        chart.removeAllSeries()
        for axis in chart.axes():
            chart.removeAxis(axis)
        increasing_color = QColor("#1a7f37")
        decreasing_color = QColor("#cf222e")
        up_series = QCandlestickSeries()
        up_series.setIncreasingColor(increasing_color)
        up_series.setDecreasingColor(increasing_color)
        up_series.setBodyOutlineVisible(False)
        up_series.setCapsVisible(False)
        down_series = QCandlestickSeries()
        down_series.setIncreasingColor(decreasing_color)
        down_series.setDecreasingColor(decreasing_color)
        down_series.setBodyOutlineVisible(False)
        down_series.setCapsVisible(False)
        for candle in candles:
            high = candle.high
            low = candle.low
            if hide_wicks:
                high = max(candle.open, candle.close)
                low = min(candle.open, candle.close)
            candle_color = increasing_color if candle.close >= candle.open else decreasing_color
            candle_set = QCandlestickSet(
                float(candle.open),
                float(high),
                float(low),
                float(candle.close),
                candle.ts,
            )
            candle_set.setBrush(candle_color)
            if candle.close >= candle.open:
                up_series.append(candle_set)
            else:
                down_series.append(candle_set)
        chart.addSeries(up_series)
        chart.addSeries(down_series)
        axis_x = QDateTimeAxis()
        axis_x.setFormat("MM-dd HH:mm")
        axis_x.setTickCount(min(8, max(2, len(candles))))
        axis_x.setRange(
            QDateTime.fromMSecsSinceEpoch(candles[0].ts),
            QDateTime.fromMSecsSinceEpoch(candles[-1].ts),
        )
        min_price = min(float(item.low if not hide_wicks else min(item.open, item.close)) for item in candles)
        max_price = max(float(item.high if not hide_wicks else max(item.open, item.close)) for item in candles)
        if min_price == max_price:
            min_price -= 1.0
            max_price += 1.0
        self._value_min = min_price
        self._value_max = max_price
        axis_y = QValueAxis()
        axis_y.setRange(min_price, max_price)
        axis_y.setLabelFormat("%.2f%%" if self._percent_axis else "%.4f")
        chart.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
        chart.addAxis(axis_y, Qt.AlignmentFlag.AlignLeft)
        up_series.attachAxis(axis_x)
        up_series.attachAxis(axis_y)
        down_series.attachAxis(axis_x)
        down_series.attachAxis(axis_y)
        ema15_values: list[Decimal] = []
        sma50_values: list[Decimal | None] = []
        if show_moving_averages:
            ema15_values, sma50_values = _build_moving_average_series(candles)
            for label, values, color, width in (
                ("EMA 15", ema15_values, "#ff4d6d", 2),
                ("SMA 50", sma50_values, "#58c66d", 2),
            ):
                series = QLineSeries()
                series.setName(label)
                series.setPen(QPen(QColor(color), width))
                for candle, value in zip(candles, values):
                    if value is None:
                        continue
                    numeric_value = float(value)
                    series.append(float(candle.ts), numeric_value)
                    self._moving_average_values.append((candle.ts, numeric_value))
                chart.addSeries(series)
                series.attachAxis(axis_x)
                series.attachAxis(axis_y)
        self._axis_x = axis_x
        self._axis_y = axis_y
        self._candles = list(candles)
        self._full_x_min_ms = float(candles[0].ts)
        self._full_x_max_ms = float(candles[-1].ts)
        self._full_y_min = float(min_price)
        self._full_y_max = float(max_price)
        self._fit_y_axis_to_visible_range()
        latest = candles[-1]
        suffix = "%" if self._percent_axis else ""
        moving_average_title = ""
        if ema15_values:
            moving_average_title += f" | EMA 15 {_format_compact_number(ema15_values[-1])}{suffix}"
        if sma50_values and sma50_values[-1] is not None:
            moving_average_title += f" | SMA 50 {_format_compact_number(sma50_values[-1])}{suffix}"
        chart.setTitle(
            f"{title} | 最新 O {_format_compact_number(latest.open)}{suffix} "
            f"H {_format_compact_number(latest.high)}{suffix} "
            f"L {_format_compact_number(latest.low)}{suffix} "
            f"C {_format_compact_number(latest.close)}{suffix}{moving_average_title}"
        )

    def wheelEvent(self, event) -> None:  # type: ignore[override]
        axis_x = self._axis_x
        if axis_x is None or not self._candles:
            super().wheelEvent(event)
            return
        plot_area = self.chart().plotArea()
        if not plot_area.contains(event.position()):
            super().wheelEvent(event)
            return
        zoom_in = event.angleDelta().y() > 0
        factor = 0.82 if zoom_in else 1.22
        x_ratio = max(0.0, min(1.0, (float(event.position().x()) - float(plot_area.left())) / max(float(plot_area.width()), 1.0)))
        current_x_min = float(axis_x.min().toMSecsSinceEpoch())
        current_x_max = float(axis_x.max().toMSecsSinceEpoch())
        current_x_span = max(current_x_max - current_x_min, 1.0)
        full_x_span = max(self._full_x_max_ms - self._full_x_min_ms, 1.0)
        candle_step_ms = max(1.0, abs(float(self._candles[1].ts - self._candles[0].ts))) if len(self._candles) > 1 else 60_000.0
        min_x_span = min(full_x_span, candle_step_ms * min(24, max(len(self._candles), 1)))
        new_x_span = max(min_x_span, min(full_x_span, current_x_span * factor))
        anchor_x = current_x_min + (current_x_span * x_ratio)
        new_x_min = anchor_x - (new_x_span * x_ratio)
        new_x_max = new_x_min + new_x_span
        if new_x_min < self._full_x_min_ms:
            new_x_min = self._full_x_min_ms
            new_x_max = new_x_min + new_x_span
        if new_x_max > self._full_x_max_ms:
            new_x_max = self._full_x_max_ms
            new_x_min = new_x_max - new_x_span
        axis_x.setRange(
            QDateTime.fromMSecsSinceEpoch(int(new_x_min)),
            QDateTime.fromMSecsSinceEpoch(int(new_x_max)),
        )
        self._fit_y_axis_to_visible_range()
        self.viewport().update()
        event.accept()

    def mouseDoubleClickEvent(self, event) -> None:  # type: ignore[override]
        self.reset_view()
        event.accept()

    def mousePressEvent(self, event) -> None:  # type: ignore[override]
        plot_area = self.chart().plotArea()
        if event.button() == Qt.MouseButton.LeftButton and plot_area.contains(event.position()):
            self._pan_anchor_x = float(event.position().x())
        super().mousePressEvent(event)

    def reset_view(self) -> None:
        if self._axis_x is not None:
            self._axis_x.setRange(
                QDateTime.fromMSecsSinceEpoch(int(self._full_x_min_ms)),
                QDateTime.fromMSecsSinceEpoch(int(self._full_x_max_ms)),
            )
        self._fit_y_axis_to_visible_range()
        self.viewport().update()

    def set_linked_hover(self, index: int | None, y_ratio: float | None) -> None:
        self._linked_hover_index = index
        self._linked_hover_y_ratio = y_ratio
        self.viewport().update()

    def mouseMoveEvent(self, event) -> None:  # type: ignore[override]
        plot_area = self.chart().plotArea()
        if self._pan_anchor_x is not None and event.buttons() & Qt.MouseButton.LeftButton:
            current_x = float(event.position().x())
            self._pan_by_pixels(current_x - self._pan_anchor_x, max(float(plot_area.width()), 1.0))
            self._pan_anchor_x = current_x
            self._hover_pos = QPointF(event.position())
            self.viewport().update()
            event.accept()
            return
        self._hover_pos = QPointF(event.position())
        if plot_area.contains(event.position()) and self._candles:
            candle = self._nearest_candle_for_x(event.position().x(), plot_area)
            if candle is not None:
                index = self._candles.index(candle)
                y_ratio = max(
                    0.0,
                    min(1.0, (float(event.position().y()) - float(plot_area.top())) / max(float(plot_area.height()), 1.0)),
                )
                self.hover_changed.emit(index, y_ratio)
        self.viewport().update()
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event) -> None:  # type: ignore[override]
        self._pan_anchor_x = None
        super().mouseReleaseEvent(event)

    def leaveEvent(self, event) -> None:  # type: ignore[override]
        self._pan_anchor_x = None
        self._hover_pos = None
        self.hover_cleared.emit()
        self._hide_hover_overlays()
        self.viewport().update()
        super().leaveEvent(event)

    def paintEvent(self, event) -> None:  # type: ignore[override]
        super().paintEvent(event)
        try:
            if not self._candles:
                self._hide_hover_overlays()
                return
            plot_area = self.chart().plotArea()
            hover_context = self._resolve_hover_context(plot_area)
            if hover_context is None:
                self._hide_hover_overlays()
                return
            candle, snapped_x, hover_y, hover_value = hover_context
            painter = QPainter(self.viewport())
            painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
            painter.setClipping(False)
            cross_pen = QPen(QColor("#6e7781"), 1)
            cross_pen.setStyle(Qt.PenStyle.DashLine)
            painter.setPen(cross_pen)
            painter.drawLine(QPointF(snapped_x, plot_area.top()), QPointF(snapped_x, plot_area.bottom()))
            painter.drawLine(QPointF(plot_area.left(), hover_y), QPointF(plot_area.right(), hover_y))
            marker_y = self._y_for_value(float(candle.close), plot_area)
            candle_color = QColor("#1a7f37" if candle.close >= candle.open else "#cf222e")
            suffix = "%" if self._percent_axis else ""
            painter.setPen(QPen(candle_color, 2))
            painter.setBrush(QColor("#ffffff"))
            painter.drawEllipse(QPointF(snapped_x, marker_y), 4.0, 4.0)
            hover_value_decimal = Decimal(str(hover_value))
            hover_value_text = _format_compact_number(hover_value_decimal)
            candle_time_text = QDateTime.fromMSecsSinceEpoch(int(candle.ts)).toString("yyyy-MM-dd HH:mm")
            if self._tooltip_close_usdt_rate is not None:
                def _price_with_usdt(label: str, value: Decimal) -> str:
                    usdt_value = value * self._tooltip_close_usdt_rate
                    return f"{label} {_format_compact_number(value)} ≈ {_format_compact_number(usdt_value)} USDT"

                tooltip_lines = [
                    candle_time_text,
                    _price_with_usdt("O", candle.open),
                    _price_with_usdt("H", candle.high),
                    _price_with_usdt("L", candle.low),
                    _price_with_usdt("C", candle.close),
                    _price_with_usdt("游标", hover_value_decimal),
                ]
                if self._tooltip_entry_price is not None:
                    tooltip_lines.append(_price_with_usdt("开仓价", self._tooltip_entry_price))
                if self._tooltip_close_usdt_basis:
                    tooltip_lines.append(f"固定基准 {self._tooltip_close_usdt_basis}")
            else:
                tooltip_lines = [
                    candle_time_text,
                    f"O {_format_compact_number(candle.open)}  H {_format_compact_number(candle.high)}",
                    f"L {_format_compact_number(candle.low)}  C {_format_compact_number(candle.close)}",
                    f"游标 {hover_value_text}{suffix}",
                ]
            painter.end()
            self._update_hover_overlays(
                bounds=plot_area,
                anchor=QPointF(snapped_x, hover_y),
                candle_color=candle_color,
                price_text=f"{hover_value_text}{suffix}",
                time_text=QDateTime.fromMSecsSinceEpoch(int(candle.ts)).toString("MM-dd HH:mm"),
                tooltip_lines=tuple(tooltip_lines),
            )
        except Exception:
            traceback.print_exc()
            self._hide_hover_overlays()
            return

    def _nearest_candle_for_x(self, x: float, plot_area: QRectF) -> Candle | None:
        if not self._candles:
            return None
        if len(self._candles) == 1:
            return self._candles[0]
        start_ts, end_ts = self._current_x_range()
        width = max(plot_area.width(), 1.0)
        ratio = max(0.0, min(1.0, (x - plot_area.left()) / width))
        target_ts = start_ts + ((end_ts - start_ts) * ratio)
        return min(self._candles, key=lambda item: abs(float(item.ts) - target_ts))

    def _resolve_hover_context(self, plot_area: QRectF) -> tuple[Candle, float, float, float] | None:
        linked_index = self._linked_hover_index
        linked_y_ratio = self._linked_hover_y_ratio
        if linked_index is not None and linked_y_ratio is not None and 0 <= linked_index < len(self._candles):
            candle = self._candles[linked_index]
            hover_y = float(plot_area.top()) + (
                min(max(float(linked_y_ratio), 0.0), 1.0) * float(max(plot_area.height(), 1.0))
            )
            return candle, self._x_for_ts(candle.ts, plot_area), hover_y, self._value_for_y(hover_y, plot_area)
        hover_pos = self._hover_pos
        if hover_pos is None or not plot_area.contains(hover_pos):
            return None
        candle = self._nearest_candle_for_x(hover_pos.x(), plot_area)
        if candle is None:
            return None
        return candle, self._x_for_ts(candle.ts, plot_area), hover_pos.y(), self._value_for_y(hover_pos.y(), plot_area)

    def _x_for_ts(self, ts: int | float, plot_area: QRectF) -> float:
        if not self._candles:
            return plot_area.left()
        start_ts, end_ts = self._current_x_range()
        if end_ts == start_ts:
            return plot_area.center().x()
        ratio = (float(ts) - start_ts) / (end_ts - start_ts)
        return plot_area.left() + (max(0.0, min(1.0, ratio)) * plot_area.width())

    def _y_for_value(self, value: float, plot_area: QRectF) -> float:
        current_min, current_max = self._current_y_range()
        span = max(current_max - current_min, 1e-9)
        ratio = (current_max - value) / span
        return plot_area.top() + (max(0.0, min(1.0, ratio)) * plot_area.height())

    def _value_for_y(self, y: float, plot_area: QRectF) -> float:
        current_min, current_max = self._current_y_range()
        ratio = max(0.0, min(1.0, (y - plot_area.top()) / max(plot_area.height(), 1.0)))
        return current_max - (ratio * (current_max - current_min))

    def _current_x_range(self) -> tuple[float, float]:
        if self._axis_x is not None:
            return float(self._axis_x.min().toMSecsSinceEpoch()), float(self._axis_x.max().toMSecsSinceEpoch())
        return self._full_x_min_ms, self._full_x_max_ms

    def _current_y_range(self) -> tuple[float, float]:
        if self._axis_y is not None:
            return float(self._axis_y.min()), float(self._axis_y.max())
        return self._value_min, self._value_max

    def _fit_y_axis_to_visible_range(self) -> None:
        axis_y = self._axis_y
        if axis_y is None or not self._candles:
            return
        start_ts, end_ts = self._current_x_range()
        visible = [item for item in self._candles if start_ts <= float(item.ts) <= end_ts]
        if not visible:
            visible = list(self._candles)
        lows = [float(min(item.open, item.close) if self._hide_wicks else item.low) for item in visible]
        highs = [float(max(item.open, item.close) if self._hide_wicks else item.high) for item in visible]
        visible_moving_averages = [
            value
            for ts, value in self._moving_average_values
            if start_ts <= float(ts) <= end_ts
        ]
        lows.extend(visible_moving_averages)
        highs.extend(visible_moving_averages)
        min_price = min(lows)
        max_price = max(highs)
        if min_price == max_price:
            padding = max(abs(min_price) * 0.02, 1e-6)
            min_price -= padding
            max_price += padding
        else:
            padding = max((max_price - min_price) * 0.06, 1e-6)
            min_price -= padding
            max_price += padding
        axis_y.setRange(min_price, max_price)

    def _pan_by_pixels(self, delta_px: float, plot_width: float) -> None:
        axis_x = self._axis_x
        if axis_x is None or not self._candles:
            return
        current_x_min = float(axis_x.min().toMSecsSinceEpoch())
        current_x_max = float(axis_x.max().toMSecsSinceEpoch())
        current_x_span = max(current_x_max - current_x_min, 1.0)
        shift_ms = (float(delta_px) / max(float(plot_width), 1.0)) * current_x_span
        new_x_min = current_x_min - shift_ms
        new_x_max = current_x_max - shift_ms
        if new_x_min < self._full_x_min_ms:
            new_x_min = self._full_x_min_ms
            new_x_max = new_x_min + current_x_span
        if new_x_max > self._full_x_max_ms:
            new_x_max = self._full_x_max_ms
            new_x_min = new_x_max - current_x_span
        axis_x.setRange(
            QDateTime.fromMSecsSinceEpoch(int(new_x_min)),
            QDateTime.fromMSecsSinceEpoch(int(new_x_max)),
        )
        self._fit_y_axis_to_visible_range()

    def _create_hover_label(self, *, multiline: bool, center: bool) -> QLabel:
        label = QLabel(self)
        label.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        label.setVisible(False)
        font = label.font()
        font.setFamily("Consolas")
        font.setPointSize(8)
        font.setBold(True)
        label.setFont(font)
        label.setWordWrap(multiline)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter if center else Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        label.setStyleSheet(
            "QLabel {"
            "background-color: rgba(15, 23, 42, 238);"
            "color: #f8fafc;"
            "border: 1px solid #334155;"
            "border-radius: 5px;"
            "padding: 4px 8px;"
            "}"
        )
        return label

    def _hide_hover_overlays(self) -> None:
        self._price_badge.hide()
        self._time_badge.hide()
        self._tooltip_badge.hide()

    def _update_hover_overlays(
        self,
        *,
        bounds: QRectF,
        anchor: QPointF,
        candle_color: QColor,
        price_text: str,
        time_text: str,
        tooltip_lines: tuple[str, ...],
    ) -> None:
        viewport_geom = self.viewport().geometry()
        viewport = QRectF(self.rect())
        mapped_bounds = QRectF(
            float(viewport_geom.left()) + float(bounds.left()),
            float(viewport_geom.top()) + float(bounds.top()),
            float(bounds.width()),
            float(bounds.height()),
        )
        mapped_anchor = QPointF(
            float(viewport_geom.left()) + float(anchor.x()),
            float(viewport_geom.top()) + float(anchor.y()),
        )
        self._price_badge.setText(price_text)
        self._price_badge.adjustSize()
        price_size = self._price_badge.sizeHint()
        price_x = max(4.0, float(mapped_bounds.left()) - float(price_size.width()) - 8.0)
        price_y = max(
            float(viewport.top()) + 4.0,
            min(
                float(mapped_anchor.y()) - (float(price_size.height()) / 2.0),
                float(viewport.bottom()) - float(price_size.height()) - 4.0,
            ),
        )
        self._price_badge.move(int(round(price_x)), int(round(price_y)))
        self._price_badge.raise_()
        self._price_badge.show()

        self._time_badge.setText(time_text)
        self._time_badge.adjustSize()
        time_size = self._time_badge.sizeHint()
        time_x = max(
            float(viewport.left()) + 4.0,
            min(
                float(mapped_anchor.x()) - (float(time_size.width()) / 2.0),
                float(viewport.right()) - float(time_size.width()) - 4.0,
            ),
        )
        time_y = min(
            float(viewport.bottom()) - float(time_size.height()) - 4.0,
            float(mapped_bounds.bottom()) + 8.0,
        )
        self._time_badge.move(int(round(time_x)), int(round(time_y)))
        self._time_badge.raise_()
        self._time_badge.show()

        tooltip_text = "\n".join(tooltip_lines)
        self._tooltip_badge.setStyleSheet(
            "QLabel {"
            "background-color: rgba(11, 18, 32, 236);"
            f"border: 1px solid {candle_color.name()};"
            "color: #f8fafc;"
            "border-radius: 7px;"
            "padding: 6px 10px;"
            "}"
        )
        self._tooltip_badge.setText(tooltip_text)
        self._tooltip_badge.adjustSize()
        tooltip_size = self._tooltip_badge.sizeHint()
        place_right = float(mapped_anchor.x()) <= float(mapped_bounds.center().x())
        place_above = float(mapped_anchor.y()) > float(mapped_bounds.center().y())
        tooltip_x = (
            float(mapped_anchor.x()) + 18.0
            if place_right
            else float(mapped_anchor.x()) - float(tooltip_size.width()) - 18.0
        )
        tooltip_y = (
            float(mapped_anchor.y()) - float(tooltip_size.height()) - 18.0
            if place_above
            else float(mapped_anchor.y()) + 18.0
        )
        tooltip_x = max(
            float(mapped_bounds.left()) + 8.0,
            min(tooltip_x, float(mapped_bounds.right()) - float(tooltip_size.width()) - 8.0),
        )
        tooltip_y = max(
            float(mapped_bounds.top()) + 8.0,
            min(tooltip_y, float(mapped_bounds.bottom()) - float(tooltip_size.height()) - 8.0),
        )
        self._tooltip_badge.move(int(round(tooltip_x)), int(round(tooltip_y)))
        self._tooltip_badge.raise_()
        self._tooltip_badge.show()

    def _draw_candle_hover_tooltip(
        self,
        painter: QPainter,
        *,
        bounds: QRectF,
        anchor: QPointF,
        marker_color: QColor,
        lines: tuple[str, ...],
    ) -> None:
        font = painter.font()
        font.setFamily("Consolas")
        font.setPointSize(8)
        font.setBold(True)
        painter.setFont(font)
        metrics = painter.fontMetrics()
        padding_x = 10.0
        padding_y = 8.0
        line_height = metrics.height() + 1
        text_width = max(metrics.horizontalAdvance(line) for line in lines)
        box_width = text_width + (padding_x * 2)
        box_height = (line_height * len(lines)) + (padding_y * 2)
        place_right = anchor.x() <= bounds.center().x()
        place_above = anchor.y() > bounds.center().y()
        box_left = anchor.x() + 18.0 if place_right else anchor.x() - box_width - 18.0
        box_top = anchor.y() - box_height - 18.0 if place_above else anchor.y() + 18.0
        box_left = max(bounds.left() + 8.0, min(box_left, bounds.right() - box_width - 8.0))
        box_top = max(bounds.top() + 8.0, min(box_top, bounds.bottom() - box_height - 8.0))
        box_rect = QRectF(box_left, box_top, box_width, box_height)
        shadow_rect = box_rect.translated(2.0, 2.0)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QColor(15, 23, 42, 55))
        painter.drawRoundedRect(shadow_rect, 7.0, 7.0)
        guide_y = max(box_rect.top() + 10.0, min(anchor.y(), box_rect.bottom() - 10.0))
        guide_x = box_rect.left() if place_right else box_rect.right()
        painter.setPen(QPen(marker_color.lighter(120), 1))
        painter.drawLine(anchor, QPointF(guide_x, guide_y))
        painter.setPen(QPen(marker_color.lighter(115), 1))
        painter.setBrush(QColor(11, 18, 32, 236))
        painter.drawRoundedRect(box_rect, 7.0, 7.0)
        text_top = box_top + padding_y + metrics.ascent()
        for index, line in enumerate(lines):
            painter.setPen(QColor("#cbd5e1") if index == 0 else QColor("#f8fafc"))
            painter.drawText(QPointF(box_left + padding_x, text_top), line)
            text_top += line_height

    def _draw_axis_badge(self, painter: QPainter, *, text: str, anchor: QPointF, side: str) -> None:
        font = painter.font()
        font.setFamily("Consolas")
        font.setPointSize(8)
        font.setBold(True)
        painter.setFont(font)
        metrics = painter.fontMetrics()
        padding_x = 9.0
        padding_y = 5.0
        badge_width = metrics.horizontalAdvance(text) + (padding_x * 2)
        badge_height = metrics.height() + (padding_y * 2)
        viewport = QRectF(self.viewport().rect())
        if side == "left":
            badge_left = max(4.0, anchor.x() - badge_width - 8.0)
            badge_top = max(viewport.top() + 4.0, min(anchor.y() - (badge_height / 2), viewport.bottom() - badge_height - 4.0))
        else:
            badge_left = max(viewport.left() + 4.0, min(anchor.x() - (badge_width / 2), viewport.right() - badge_width - 4.0))
            badge_top = min(viewport.bottom() - badge_height - 4.0, anchor.y() + 8.0)
        badge_rect = QRectF(badge_left, badge_top, badge_width, badge_height)
        painter.setPen(QPen(QColor("#334155"), 1))
        painter.setBrush(QColor(15, 23, 42, 238))
        painter.drawRoundedRect(badge_rect, 5.0, 5.0)
        painter.setPen(QColor("#ffffff"))
        text_x = badge_left + ((badge_width - metrics.horizontalAdvance(text)) / 2.0)
        text_y = badge_top + ((badge_height - metrics.height()) / 2.0) + metrics.ascent()
        painter.drawText(QPointF(text_x, text_y), text)


class OptionStrategyBigChartDialog(QDialog):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        apply_qt_window_icon(self)
        self.setWindowFlag(Qt.WindowType.WindowMaximizeButtonHint, True)
        self.setWindowFlag(Qt.WindowType.WindowCloseButtonHint, True)
        self.setWindowTitle("期权策略图表大窗")
        self.resize(1560, 980)
        layout = QVBoxLayout(self)
        self._tabs = QTabWidget()
        layout.addWidget(self._tabs, 1)

        self._payoff_note = QLabel("")
        self._payoff_note.setWordWrap(True)
        self._payoff_chart = PayoffChartView()
        payoff_page = QWidget()
        payoff_layout = QVBoxLayout(payoff_page)
        payoff_layout.addWidget(self._payoff_note)
        payoff_layout.addWidget(self._payoff_chart, 1)
        self._tabs.addTab(payoff_page, "到期盈亏图")

        self._combo_note = QLabel("")
        self._combo_note.setWordWrap(True)
        self._combo_chart = CandlestickChartView()
        combo_page = QWidget()
        combo_layout = QVBoxLayout(combo_page)
        combo_layout.addWidget(self._combo_note)
        combo_layout.addWidget(self._combo_chart, 1)
        self._tabs.addTab(combo_page, "组合K线")

        self._vol_note = QLabel("")
        self._vol_note.setWordWrap(True)
        self._vol_chart = CandlestickChartView(percent_axis=True)
        vol_page = QWidget()
        vol_layout = QVBoxLayout(vol_page)
        vol_layout.addWidget(self._vol_note)
        vol_layout.addWidget(self._vol_chart, 1)
        self._tabs.addTab(vol_page, "波动率K线")

        overlay_page = QWidget()
        overlay_layout = QVBoxLayout(overlay_page)
        toolbar = QHBoxLayout()
        toolbar.addWidget(QLabel("叠加对比周期"))
        self._overlay_period_combo = QComboBox()
        self._overlay_period_combo.addItem("1小时", "1H")
        self._overlay_period_combo.addItem("4小时", "4H")
        self._overlay_period_combo.addItem("日线", "1D")
        toolbar.addWidget(self._overlay_period_combo)
        toolbar.addStretch(1)
        self._overlay_refresh_button = QPushButton("刷新叠加对比")
        toolbar.addWidget(self._overlay_refresh_button)
        overlay_layout.addLayout(toolbar)
        self._overlay_note = QLabel("")
        self._overlay_note.setWordWrap(True)
        overlay_layout.addWidget(self._overlay_note)
        self._overlay_combo_chart = CandlestickChartView()
        self._overlay_vol_chart = CandlestickChartView(percent_axis=True)
        self._overlay_spot_chart = CandlestickChartView()
        overlay_layout.addWidget(self._overlay_combo_chart, 1)
        overlay_layout.addWidget(self._overlay_vol_chart, 1)
        overlay_layout.addWidget(self._overlay_spot_chart, 1)
        self._tabs.addTab(overlay_page, "叠加对比")

    @property
    def overlay_period(self) -> str:
        return str(self._overlay_period_combo.currentData() or "1H")


class OptionStrategyQtWindow(QMainWindow):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("期权策略计算器")
        self.resize(1880, 1180)
        self._client = _shared_client()
        self._saved_strategies: list[dict[str, object]] = []
        self._all_option_instruments: list[Instrument] = []
        self._family_instruments_cache: dict[str, list[Instrument]] = {}
        self._family_tickers_cache: dict[str, dict[str, OkxTicker]] = {}
        self._instrument_map: dict[str, Instrument] = {}
        self._quotes_by_inst_id: dict[str, OptionQuote] = {}
        self._chain_rows: list[OptionChainRow] = []
        self._legs: list[StrategyLegDefinition] = []
        self._current_underlying_price: Decimal | None = None
        self._latest_spot_usdt_price: Decimal | None = None
        self._latest_spot_usdt_candles: list[Candle] = []
        self._latest_deribit_volatility_candles: list[Candle] = []
        self._latest_deribit_resolution_label = ""
        self._latest_deribit_resolution_note = ""
        self._latest_combo_candles: list[Candle] = []
        self._latest_payoff_snapshot: StrategyPayoffSnapshot | None = None
        self._latest_expiry_payoff_snapshot: StrategyPayoffSnapshot | None = None
        self._latest_combo_value: Decimal | None = None
        self._latest_combo_requested_limit: int | None = None
        self._latest_combo_source_counts: dict[str, int] = {}
        self._latest_chart_formula = ""
        self._latest_resolved_legs: list[ResolvedStrategyLeg] = []
        self._latest_implied_volatility_by_alias: dict[str, Decimal] = {}
        self._latest_payoff_loaded_at: datetime | None = None
        self._latest_payoff_expiry_at: datetime | None = None
        self._alias_counter = 0
        self._profile_name = "159"
        self._chain_request_id = 0
        self._position_import_request_id = 0
        self._chart_request_id = 0
        self._overlay_chart_request_id = 0
        self._worker_threads: dict[str, QThread] = {}
        self._big_dialog: OptionStrategyBigChartDialog | None = None
        self._overlay_triples: list[tuple[Candle, Candle, Candle]] = []
        self._overlay_combo_ccy = ""
        self._overlay_spot_inst_id = ""
        self._overlay_vol_currency = ""
        self._overlay_resolution_label = ""
        self._overlay_resolution_note = ""
        self._overlay_hover_index: int | None = None
        self._overlay_hover_y_ratio: float | None = None

        self._build_ui()
        self._load_saved_strategies()
        self._refresh_saved_strategy_options()
        self._seed_family_options()
        QTimer.singleShot(150, self.refresh_chain)

    def closeEvent(self, event) -> None:  # type: ignore[override]
        for thread in list(self._worker_threads.values()):
            thread.wait(100)
        super().closeEvent(event)

    def _build_ui(self) -> None:
        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        header = QHBoxLayout()
        title = QLabel("期权策略计算器")
        title.setObjectName("SectionTitle")
        self._status_label = QLabel("正在加载期权系列...")
        self._status_label.setObjectName("Subtle")
        self._status_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        header.addWidget(title, 1)
        header.addWidget(self._status_label, 1)
        layout.addLayout(header)

        controls = QGroupBox("策略设置")
        controls_layout = QGridLayout(controls)
        controls_layout.setHorizontalSpacing(16)
        controls_layout.setVerticalSpacing(12)

        strategy_box = QGroupBox("策略")
        strategy_form = QGridLayout(strategy_box)
        strategy_form.addWidget(QLabel("策略名称"), 0, 0)
        self._strategy_name_edit = QLineEdit()
        strategy_form.addWidget(self._strategy_name_edit, 0, 1, 1, 3)
        strategy_form.addWidget(QLabel("已保存策略"), 1, 0)
        self._saved_strategy_combo = QComboBox()
        strategy_form.addWidget(self._saved_strategy_combo, 1, 1)
        load_button = QPushButton("加载")
        save_button = QPushButton("保存")
        delete_button = QPushButton("删除")
        load_button.clicked.connect(self.load_selected_strategy)
        save_button.clicked.connect(self.save_current_strategy)
        delete_button.clicked.connect(self.delete_selected_strategy)
        strategy_form.addWidget(load_button, 1, 2)
        strategy_form.addWidget(save_button, 1, 3)
        strategy_form.addWidget(delete_button, 1, 4)

        market_box = QGroupBox("期权链")
        market_form = QGridLayout(market_box)
        market_form.addWidget(QLabel("期权系列"), 0, 0)
        self._family_combo = QComboBox()
        self._family_combo.setEditable(True)
        self._family_combo.currentTextChanged.connect(self._on_family_changed)
        market_form.addWidget(self._family_combo, 0, 1)
        market_form.addWidget(QLabel("到期日"), 0, 2)
        self._expiry_combo = QComboBox()
        self._expiry_combo.setMinimumWidth(240)
        self._expiry_combo.setMinimumContentsLength(18)
        self._expiry_combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon)
        self._expiry_combo.currentTextChanged.connect(self._on_expiry_changed)
        market_form.addWidget(self._expiry_combo, 0, 3)
        refresh_chain_button = QPushButton("刷新期权链")
        refresh_chain_button.clicked.connect(self.refresh_chain)
        market_form.addWidget(refresh_chain_button, 0, 4)
        import_expiry_button = QPushButton("导入到期持仓")
        import_expiry_button.clicked.connect(lambda: self._start_import_positions(scope="expiry"))
        market_form.addWidget(import_expiry_button, 0, 5)
        import_family_button = QPushButton("导入系列持仓")
        import_family_button.clicked.connect(lambda: self._start_import_positions(scope="family"))
        market_form.addWidget(import_family_button, 0, 6)
        market_form.addWidget(QLabel("默认数量"), 1, 0)
        self._default_qty_edit = QLineEdit("1")
        market_form.addWidget(self._default_qty_edit, 1, 1)
        market_form.addWidget(QLabel("先选系列并刷新；也可直接把当前到期日或整个系列的持仓导入策略腿。"), 1, 2, 1, 5)

        formula_box = QGroupBox("图表与公式")
        formula_layout = QGridLayout(formula_box)
        formula_layout.addWidget(QLabel("组合公式"), 0, 0)
        self._formula_edit = QLineEdit()
        formula_layout.addWidget(self._formula_edit, 0, 1)
        default_formula_button = QPushButton("默认公式")
        default_formula_button.clicked.connect(self.use_default_formula)
        refresh_chart_button = QPushButton("刷新图表")
        refresh_chart_button.clicked.connect(self.refresh_charts)
        big_chart_button = QPushButton("图表大窗")
        big_chart_button.clicked.connect(self.open_big_chart_window)
        formula_layout.addWidget(default_formula_button, 0, 2)
        formula_layout.addWidget(refresh_chart_button, 0, 3)
        formula_layout.addWidget(big_chart_button, 0, 4)
        formula_layout.addWidget(QLabel("公式支持线性表达式，例如 L1 - 2*L2 + 0.5。"), 1, 0, 1, 5)

        controls_layout.addWidget(strategy_box, 0, 0)
        controls_layout.addWidget(market_box, 0, 1)
        controls_layout.addWidget(formula_box, 1, 0, 1, 2)
        layout.addWidget(controls)

        main_splitter = QSplitter(Qt.Orientation.Vertical)
        top_splitter = QSplitter(Qt.Orientation.Horizontal)

        chain_panel = QWidget()
        chain_layout = QVBoxLayout(chain_panel)
        self._chain_context_label = QLabel("选择一个行权价后，可把认购 / 认沽直接加入策略腿。")
        self._chain_context_label.setWordWrap(True)
        chain_layout.addWidget(self._chain_context_label)
        self._chain_table = QTableWidget(0, 7)
        self._chain_table.setHorizontalHeaderLabels(("认购标记", "认购买一", "认购卖一", "行权价", "认沽买一", "认沽卖一", "认沽标记"))
        self._chain_table.verticalHeader().setVisible(False)
        self._chain_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._chain_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._chain_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._chain_table.itemSelectionChanged.connect(self._on_chain_selected)
        chain_layout.addWidget(self._chain_table, 1)
        chain_actions = QHBoxLayout()
        for text, option_type, side in (
            ("添加认购买入", "C", "buy"),
            ("添加认购卖出", "C", "sell"),
            ("添加认沽买入", "P", "buy"),
            ("添加认沽卖出", "P", "sell"),
        ):
            button = QPushButton(text)
            button.clicked.connect(lambda _checked=False, ot=option_type, sd=side: self.add_selected_chain_leg(ot, sd))
            chain_actions.addWidget(button)
        chain_layout.addLayout(chain_actions)

        legs_panel = QWidget()
        legs_layout = QVBoxLayout(legs_panel)
        self._strategy_summary_label = QLabel("暂无策略腿。")
        self._strategy_summary_label.setWordWrap(True)
        legs_layout.addWidget(self._strategy_summary_label)
        self._legs_table = QTableWidget(0, 18)
        self._legs_table.setHorizontalHeaderLabels(
            (
                "别名",
                "合约",
                "类别",
                "到期日",
                "行权价",
                "买卖",
                "数量",
                "持仓价",
                "持仓价≈USDT",
                "标记价",
                "标记价≈USDT",
                "每张面值",
                "权利金合计",
                "Delta",
                "Gamma",
                "Theta",
                "Theta≈USDT",
                "Vega",
            )
        )
        self._legs_table.verticalHeader().setVisible(False)
        self._legs_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._legs_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._legs_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._legs_table.cellDoubleClicked.connect(self._on_legs_table_double_clicked)
        legs_layout.addWidget(self._legs_table, 1)
        legs_actions = QHBoxLayout()
        for text, slot in (
            ("删除选中腿", self.remove_selected_leg),
            ("修改数量", self.edit_selected_leg_quantity),
            ("修改持仓价", self.edit_selected_leg_premium),
            ("清空策略腿", self.clear_legs),
            ("刷新腿报价", self.refresh_leg_quotes),
        ):
            button = QPushButton(text)
            button.clicked.connect(slot)
            legs_actions.addWidget(button)
        legs_layout.addLayout(legs_actions)

        top_splitter.addWidget(chain_panel)
        top_splitter.addWidget(legs_panel)
        top_splitter.setSizes([520, 1040])
        main_splitter.addWidget(top_splitter)

        self._tabs = QTabWidget()
        payoff_tab = QWidget()
        payoff_layout = QVBoxLayout(payoff_tab)
        payoff_top = QHBoxLayout()
        self._payoff_summary_label = QLabel("加入策略腿后，可生成到期盈亏图。")
        self._payoff_summary_label.setWordWrap(True)
        payoff_top.addWidget(self._payoff_summary_label, 1)
        payoff_top.addWidget(QLabel("图表币种"))
        self._display_ccy_combo = QComboBox()
        self._display_ccy_combo.addItem("结算币", "结算币")
        self._display_ccy_combo.addItem("USDT", "USDT")
        self._display_ccy_combo.currentIndexChanged.connect(self._refresh_chart_display)
        payoff_top.addWidget(self._display_ccy_combo)
        payoff_recalc_button = QPushButton("重新计算")
        payoff_recalc_button.clicked.connect(self.refresh_charts)
        payoff_top.addWidget(payoff_recalc_button)
        payoff_layout.addLayout(payoff_top)
        slider_row = QHBoxLayout()
        self._payoff_sim_date_label = QLabel("估值日 -")
        slider_row.addWidget(self._payoff_sim_date_label)
        self._payoff_time_slider = QSlider(Qt.Orientation.Horizontal)
        self._payoff_time_slider.setRange(0, 100)
        self._payoff_time_slider.setValue(100)
        self._payoff_time_slider.valueChanged.connect(self._on_payoff_slider_changed)
        slider_row.addWidget(self._payoff_time_slider, 1)
        self._payoff_vol_shift_label = QLabel("波动率平移 0%")
        slider_row.addWidget(self._payoff_vol_shift_label)
        self._payoff_vol_slider = QSlider(Qt.Orientation.Horizontal)
        self._payoff_vol_slider.setRange(-70, 200)
        self._payoff_vol_slider.setValue(0)
        self._payoff_vol_slider.valueChanged.connect(self._on_payoff_slider_changed)
        slider_row.addWidget(self._payoff_vol_slider, 1)
        payoff_layout.addLayout(slider_row)
        self._payoff_chart = PayoffChartView()
        payoff_layout.addWidget(self._payoff_chart, 1)
        self._tabs.addTab(payoff_tab, "到期盈亏图")

        combo_tab = QWidget()
        combo_layout = QVBoxLayout(combo_tab)
        combo_toolbar = QHBoxLayout()
        combo_toolbar.addWidget(QLabel("K线周期"))
        self._bar_combo = QComboBox()
        for bar in BAR_OPTIONS:
            self._bar_combo.addItem(bar, bar)
        self._bar_combo.setCurrentText("1H")
        combo_toolbar.addWidget(self._bar_combo)
        combo_toolbar.addWidget(QLabel("K线数量"))
        self._candle_limit_edit = QLineEdit("1000")
        combo_toolbar.addWidget(self._candle_limit_edit)
        combo_toolbar.addWidget(QLabel("图表币种"))
        self._combo_ccy_combo = QComboBox()
        self._combo_ccy_combo.addItem("结算币", "结算币")
        self._combo_ccy_combo.addItem("USDT", "USDT")
        self._combo_ccy_combo.currentIndexChanged.connect(lambda: self._refresh_chart_display(combo_only=True))
        combo_toolbar.addWidget(self._combo_ccy_combo)
        self._hide_wicks_check = QCheckBox("消除影线")
        self._hide_wicks_check.stateChanged.connect(lambda: self._refresh_chart_display(combo_only=True))
        combo_toolbar.addWidget(self._hide_wicks_check)
        combo_toolbar.addStretch(1)
        combo_refresh_button = QPushButton("刷新组合K线")
        combo_refresh_button.clicked.connect(self.refresh_combo_chart)
        combo_toolbar.addWidget(combo_refresh_button)
        combo_layout.addLayout(combo_toolbar)
        self._combo_summary_label = QLabel("组合浮盈亏 K 线按 持仓价差*张数*每张面值 计算；上涨代表盈利，回落代表亏损。")
        self._combo_summary_label.setWordWrap(True)
        combo_layout.addWidget(self._combo_summary_label)
        self._combo_chart = CandlestickChartView()
        combo_layout.addWidget(self._combo_chart, 1)
        self._tabs.addTab(combo_tab, "组合K线")
        main_splitter.addWidget(self._tabs)
        main_splitter.setSizes([520, 460])
        layout.addWidget(main_splitter, 1)

        self._apply_table_style(self._chain_table)
        self._apply_table_style(self._legs_table)
        self._payoff_chart.show_message("加入策略腿后，可生成到期盈亏图。")
        self._combo_chart.show_message("组合浮盈亏 K 线按持仓价差计算；先加入策略腿再生成。")

    def _apply_table_style(self, table: QTableWidget) -> None:
        header = table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        header.setStretchLastSection(True)

    def _load_saved_strategies(self) -> None:
        snapshot = load_option_strategies_snapshot()
        records = snapshot.get("strategies", [])
        self._saved_strategies = [item for item in records if isinstance(item, dict)]

    def _refresh_saved_strategy_options(self) -> None:
        current = self._saved_strategy_combo.currentText().strip()
        self._saved_strategy_combo.blockSignals(True)
        self._saved_strategy_combo.clear()
        names = [str(item.get("name", "")) for item in self._saved_strategies if str(item.get("name", "")).strip()]
        self._saved_strategy_combo.addItems(names)
        self._saved_strategy_combo.blockSignals(False)
        if current:
            index = self._saved_strategy_combo.findText(current)
            if index >= 0:
                self._saved_strategy_combo.setCurrentIndex(index)

    def _seed_family_options(self) -> None:
        current = self._family_combo.currentText().strip().upper()
        values = list(DEFAULT_OPTION_FAMILY_OPTIONS)
        if current and current not in values:
            values.insert(0, current)
        self._family_combo.clear()
        self._family_combo.addItems(values)
        if current:
            self._family_combo.setCurrentText(current)
        elif values:
            self._family_combo.setCurrentText(values[0])
        self._status_label.setText("首次打开会自动刷新默认期权系列；也可以切换系列后手动刷新期权链。")
        self._update_chain_context_ui()

    def _selected_expiry_code(self) -> str:
        raw = self._expiry_combo.currentText().strip()
        if " " in raw:
            raw = raw.split(" ", 1)[0]
        if "(" in raw:
            raw = raw.split("(", 1)[0].strip()
        return raw

    def _current_chain_context_text(self) -> str:
        parts: list[str] = []
        family = self._family_combo.currentText().strip().upper()
        expiry = self._selected_expiry_code()
        if family:
            parts.append(family)
        if expiry:
            parts.append(f"{expiry} ({format_option_expiry_label(expiry)})")
        return " | ".join(parts)

    def _update_chain_context_ui(self, row_count: int | None = None) -> None:
        context = self._current_chain_context_text()
        if row_count is not None:
            context = f"{context} | {row_count} 个行权价" if context else f"{row_count} 个行权价"
        if not context:
            context = "选择一个行权价后，可把认购 / 认沽直接加入策略腿。"
        self._chain_context_label.setText(context)

    @Slot()
    def _on_family_changed(self) -> None:
        self._chain_rows = []
        self._chain_table.setRowCount(0)
        self._sync_expiry_options()
        self._status_label.setText("点击“刷新期权链”后，会把当前系列的所有到期日刷新出来。")

    @Slot()
    def _on_expiry_changed(self) -> None:
        if getattr(self, "_syncing_expiry_combo", False):
            return
        if self._selected_expiry_code():
            if not self._apply_cached_expiry_selection():
                self.refresh_chain()

    def _sync_expiry_options(self, *, preferred: str | None = None) -> None:
        family = self._family_combo.currentText().strip().upper()
        instruments = list(self._family_instruments_cache.get(family, ()))
        expiries = sorted({parse_option_contract(item.inst_id).expiry_code for item in instruments})
        display_values = [f"{code} ({format_option_expiry_label(code)})" for code in expiries]
        current = preferred or self._selected_expiry_code()
        self._syncing_expiry_combo = True
        self._expiry_combo.blockSignals(True)
        try:
            self._expiry_combo.clear()
            self._expiry_combo.addItems(display_values)
            if current in expiries:
                self._expiry_combo.setCurrentText(f"{current} ({format_option_expiry_label(current)})")
            elif display_values:
                self._expiry_combo.setCurrentIndex(0)
        finally:
            self._expiry_combo.blockSignals(False)
            self._syncing_expiry_combo = False
        self._update_chain_context_ui()

    def _apply_cached_expiry_selection(self) -> bool:
        family = self._family_combo.currentText().strip().upper()
        expiry = self._selected_expiry_code()
        family_instruments = self._family_instruments_cache.get(family)
        tickers_by_inst_id = self._family_tickers_cache.get(family)
        if not family or not expiry or not family_instruments or not tickers_by_inst_id:
            return False
        selected_instruments = [item for item in family_instruments if parse_option_contract(item.inst_id).expiry_code == expiry]
        if not selected_instruments:
            return False
        quotes = [_build_option_quote(item, tickers_by_inst_id.get(item.inst_id)) for item in selected_instruments]
        snapshot = ChainSnapshot(
            family=family,
            expiry=expiry,
            expiries=tuple(sorted({parse_option_contract(item.inst_id).expiry_code for item in family_instruments})),
            chain_rows=tuple(build_option_chain_rows(quotes)),
            quotes=tuple(quotes),
            family_instruments=tuple(family_instruments),
            tickers_by_inst_id=dict(tickers_by_inst_id),
            underlying_price=next((item.index_price for item in quotes if item.index_price is not None), self._current_underlying_price),
        )
        self._apply_chain_snapshot(self._chain_request_id, snapshot)
        self._status_label.setText(f"已切换到 {family} {expiry}，期权链已按当前到期日更新。")
        return True

    def _start_thread(self, key: str, thread: QThread) -> None:
        old = self._worker_threads.pop(key, None)
        if old is not None:
            old.wait(50)
        thread.finished.connect(lambda: self._worker_threads.pop(key, None))
        self._worker_threads[key] = thread
        thread.start()

    def _selected_chain_row(self) -> OptionChainRow | None:
        row = self._chain_table.currentRow()
        if row < 0 or row >= len(self._chain_rows):
            return None
        return self._chain_rows[row]

    def _selected_leg_index(self) -> int | None:
        row = self._legs_table.currentRow()
        if row < 0 or row >= len(self._legs):
            return None
        return row

    @Slot()
    def refresh_chain(self) -> None:
        family = self._family_combo.currentText().strip().upper()
        if not family:
            QMessageBox.warning(self, "期权链参数错误", "请先输入或选择期权系列。")
            return
        self._chain_request_id += 1
        request_id = self._chain_request_id
        self._status_label.setText(f"正在加载 {family} 全部到期日...")
        thread = _OptionChainThread(
            request_id=request_id,
            family=family,
            preferred_expiry=self._selected_expiry_code(),
            client=self._client,
            parent=self,
        )
        thread.snapshot_ready.connect(self._apply_chain_snapshot)
        thread.error_raised.connect(self._show_chain_error)
        self._start_thread("chain", thread)

    @Slot(int, object)
    def _apply_chain_snapshot(self, request_id: int, snapshot: object) -> None:
        if request_id != self._chain_request_id and request_id != self._chain_request_id + 1:
            return
        if not isinstance(snapshot, ChainSnapshot):
            return
        self._chain_rows = list(snapshot.chain_rows)
        self._current_underlying_price = snapshot.underlying_price
        self._family_instruments_cache[snapshot.family] = list(snapshot.family_instruments)
        self._family_tickers_cache[snapshot.family] = dict(snapshot.tickers_by_inst_id)
        self._all_option_instruments = list(snapshot.family_instruments)
        for instrument in snapshot.family_instruments:
            self._instrument_map[instrument.inst_id] = instrument
        for quote in snapshot.quotes:
            self._quotes_by_inst_id[quote.instrument.inst_id] = quote
        self._sync_expiry_options(preferred=snapshot.expiry)
        self._render_chain_rows()
        self._status_label.setText(f"{snapshot.family} 已刷新出 {len(snapshot.expiries)} 个到期日，当前显示 {snapshot.expiry or '-'}。")
        self._refresh_strategy_summary()

    @Slot(int, str)
    def _show_chain_error(self, request_id: int, message: str) -> None:
        if request_id != self._chain_request_id:
            return
        self._status_label.setText("期权链加载失败")
        QMessageBox.critical(self, "期权链加载失败", message)

    def _render_chain_rows(self) -> None:
        self._chain_table.setRowCount(len(self._chain_rows))
        for row_index, row in enumerate(self._chain_rows):
            call_tick = row.call_quote.instrument.tick_size if row.call_quote is not None else None
            put_tick = row.put_quote.instrument.tick_size if row.put_quote is not None else None
            values = (
                _format_price(row.call_quote.mark_price if row.call_quote is not None else None, call_tick),
                _format_price(row.call_quote.bid_price if row.call_quote is not None else None, call_tick),
                _format_price(row.call_quote.ask_price if row.call_quote is not None else None, call_tick),
                format_decimal(row.strike),
                _format_price(row.put_quote.bid_price if row.put_quote is not None else None, put_tick),
                _format_price(row.put_quote.ask_price if row.put_quote is not None else None, put_tick),
                _format_price(row.put_quote.mark_price if row.put_quote is not None else None, put_tick),
            )
            for column_index, value in enumerate(values):
                self._chain_table.setItem(row_index, column_index, QTableWidgetItem(str(value)))
        self._update_chain_context_ui(row_count=len(self._chain_rows))
        if self._chain_rows:
            self._chain_table.selectRow(0)
            self._on_chain_selected()
        else:
            self._chain_context_label.setText("当前到期日没有拿到可用期权链数据。")

    @Slot()
    def _on_chain_selected(self) -> None:
        row = self._selected_chain_row()
        if row is None:
            return
        call_inst_id = row.call_quote.instrument.inst_id if row.call_quote is not None else "-"
        put_inst_id = row.put_quote.instrument.inst_id if row.put_quote is not None else "-"
        current_price = f" | 标的指数≈{format_decimal(self._current_underlying_price)}" if self._current_underlying_price else ""
        context = self._current_chain_context_text()
        self._chain_context_label.setText(
            f"{context} | 行权价 {format_decimal(row.strike)} | 认购 {call_inst_id} | 认沽 {put_inst_id}{current_price}"
        )

    def _parse_positive_decimal(self, text: str, field_name: str) -> Decimal:
        try:
            value = Decimal(text.strip())
        except (InvalidOperation, AttributeError) as exc:
            raise ValueError(f"{field_name} 不是有效数字。") from exc
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0。")
        return value

    def _parse_positive_int(self, text: str, field_name: str) -> int:
        try:
            value = int(text.strip())
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"{field_name} 不是有效整数。") from exc
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0。")
        return value

    @Slot()
    def add_selected_chain_leg(self, option_type: str, side: str) -> None:
        row = self._selected_chain_row()
        if row is None:
            QMessageBox.information(self, "添加策略腿", "请先在期权链里选择一个行权价。")
            return
        quote = row.call_quote if option_type == "C" else row.put_quote
        if quote is None:
            QMessageBox.information(self, "添加策略腿", "当前行权价没有对应的可用合约。")
            return
        try:
            quantity = self._parse_positive_decimal(self._default_qty_edit.text(), "默认数量")
        except Exception as exc:  # noqa: BLE001
            QMessageBox.critical(self, "数量错误", str(exc))
            return
        self._alias_counter += 1
        leg = StrategyLegDefinition(
            alias=f"L{self._alias_counter}",
            inst_id=quote.instrument.inst_id,
            side="buy" if side == "buy" else "sell",
            quantity=quantity,
            premium=quote.reference_price,
            enabled=True,
        )
        self._legs.append(leg)
        self._instrument_map[quote.instrument.inst_id] = quote.instrument
        self._quotes_by_inst_id[quote.instrument.inst_id] = quote
        if self._current_underlying_price is None:
            self._current_underlying_price = quote.index_price or self._load_spot_reference_price_for_legs(self._legs)
        if not self._formula_edit.text().strip():
            self._formula_edit.setText(build_default_formula(self._legs))
        self._refresh_leg_greeks()
        self._render_legs()
        self._refresh_strategy_summary()

    @Slot()
    def remove_selected_leg(self) -> None:
        index = self._selected_leg_index()
        if index is None:
            QMessageBox.information(self, "删除策略腿", "请先选择一条策略腿。")
            return
        self._legs.pop(index)
        self._render_legs()
        self._refresh_strategy_summary()

    @Slot(int, int)
    def _on_legs_table_double_clicked(self, row: int, column: int) -> None:
        self._legs_table.selectRow(row)
        if column == 6:
            self.edit_selected_leg_quantity()
        elif column == 7:
            self.edit_selected_leg_premium()

    @Slot()
    def edit_selected_leg_quantity(self) -> None:
        index = self._selected_leg_index()
        if index is None:
            QMessageBox.information(self, "修改数量", "请先选择一条策略腿。")
            return
        leg = self._legs[index]
        old_default_formula = build_default_formula(self._legs)
        text, accepted = QInputDialog.getText(self, "修改数量", f"请输入 {leg.alias} 的新数量：", text=format_decimal(leg.quantity))
        if not accepted:
            return
        try:
            leg.quantity = self._parse_positive_decimal(text, "策略腿数量")
        except Exception as exc:  # noqa: BLE001
            QMessageBox.critical(self, "修改数量失败", str(exc))
            return
        if self._formula_edit.text().strip() == old_default_formula:
            self._formula_edit.setText(build_default_formula(self._legs))
        self._render_legs()
        self._refresh_strategy_summary()
        if self._latest_payoff_snapshot is not None or self._latest_combo_candles:
            self.refresh_charts()

    @Slot()
    def edit_selected_leg_premium(self) -> None:
        index = self._selected_leg_index()
        if index is None:
            QMessageBox.information(self, "修改持仓价", "请先选择一条策略腿。")
            return
        leg = self._legs[index]
        initial = format_decimal(leg.premium) if leg.premium is not None else ""
        text, accepted = QInputDialog.getText(self, "修改持仓价", f"请输入 {leg.alias} 的持仓价：", text=initial)
        if not accepted:
            return
        text = text.strip()
        if not text:
            leg.premium = None
        else:
            try:
                leg.premium = self._parse_positive_decimal(text, "持仓价")
            except Exception as exc:  # noqa: BLE001
                QMessageBox.critical(self, "修改持仓价失败", str(exc))
                return
        self._render_legs()
        self._refresh_strategy_summary()
        if self._latest_payoff_snapshot is not None or self._latest_combo_candles:
            self._refresh_payoff_simulation()
            self._refresh_chart_display()

    @Slot()
    def clear_legs(self) -> None:
        self._legs.clear()
        self._latest_payoff_snapshot = None
        self._latest_expiry_payoff_snapshot = None
        self._latest_combo_candles = []
        self._latest_combo_value = None
        self._latest_spot_usdt_price = None
        self._latest_spot_usdt_candles = []
        self._latest_chart_formula = ""
        self._latest_resolved_legs = []
        self._latest_implied_volatility_by_alias = {}
        self._latest_payoff_loaded_at = None
        self._latest_payoff_expiry_at = None
        self._reset_payoff_simulation_controls()
        self._render_legs()
        self._refresh_strategy_summary()
        self._payoff_chart.show_message("加入策略腿后，可生成到期盈亏图。")
        self._combo_chart.show_message("组合浮盈亏 K 线按持仓价差计算；先加入策略腿再生成。")
        self._payoff_summary_label.setText("加入策略腿后，可生成到期盈亏图。")
        self._combo_summary_label.setText("组合浮盈亏 K 线按 持仓价差*张数*每张面值 计算；上涨代表盈利，回落代表亏损。")

    def _leg_mark_price(self, inst_id: str) -> Decimal | None:
        quote = self._quotes_by_inst_id.get(inst_id)
        return quote.reference_price if quote is not None else None

    def _option_value_approx_usdt(self, inst_id: str, value: Decimal | None) -> Decimal | None:
        if value is None:
            return None
        quote_currency = _strategy_leg_quote_currency(inst_id, self._instrument_map)
        if quote_currency in {"USDT", "USD", "USDC"}:
            return value
        reference_price = self._current_underlying_price
        if reference_price is None or reference_price <= 0:
            return None
        return value * reference_price

    def _refresh_leg_greeks(self) -> None:
        valuation_time = datetime.now()
        settlement_price = self._current_underlying_price
        for leg in self._legs:
            leg.delta = None
            leg.gamma = None
            leg.theta = None
            leg.vega = None
            instrument = self._instrument_map.get(leg.inst_id)
            quote = self._quotes_by_inst_id.get(leg.inst_id)
            if instrument is None or quote is None or quote.reference_price is None or settlement_price is None or settlement_price <= 0 or leg.premium is None:
                continue
            try:
                resolved_leg = resolve_strategy_leg(leg, instrument)
                implied_volatility = infer_implied_volatility_for_leg(
                    resolved_leg,
                    settlement_price=settlement_price,
                    valuation_time=valuation_time,
                    option_price=quote.reference_price,
                )
                greeks = estimate_leg_greeks(
                    resolved_leg,
                    settlement_price=settlement_price,
                    valuation_time=valuation_time,
                    base_implied_volatility=implied_volatility,
                )
                direction = Decimal("1") if leg.side == "buy" else Decimal("-1")
                leg.delta = greeks["delta"] * direction * leg.quantity
                leg.gamma = greeks["gamma"] * direction * leg.quantity
                leg.theta = greeks["theta"] * direction * leg.quantity
                leg.vega = greeks["vega"] * direction * leg.quantity
            except Exception:
                continue

    def _render_legs(self) -> None:
        self._legs_table.setRowCount(len(self._legs))
        for row_index, leg in enumerate(self._legs):
            instrument = self._instrument_map.get(leg.inst_id)
            parsed = parse_option_contract(leg.inst_id)
            premium = leg.premium
            mark_price = self._leg_mark_price(leg.inst_id)
            premium_usdt = self._option_value_approx_usdt(leg.inst_id, premium)
            mark_price_usdt = self._option_value_approx_usdt(leg.inst_id, mark_price)
            theta_usdt = self._option_value_approx_usdt(leg.inst_id, leg.theta)
            contract_value = option_contract_value(instrument) if instrument is not None else Decimal("1")
            premium_total = premium * contract_value * leg.quantity if premium is not None else None
            values = (
                leg.alias,
                leg.inst_id,
                "认购" if parsed.option_type == "C" else "认沽",
                parsed.expiry_label,
                format_decimal(parsed.strike),
                "买入" if leg.side == "buy" else "卖出",
                format_decimal(leg.quantity),
                _format_price(premium, instrument.tick_size if instrument is not None else None),
                _format_compact_number(premium_usdt),
                _format_price(mark_price, instrument.tick_size if instrument is not None else None),
                _format_compact_number(mark_price_usdt),
                format_decimal(contract_value),
                format_decimal(premium_total) if premium_total is not None else "-",
                _format_compact_number(leg.delta),
                _format_compact_number(leg.gamma),
                _format_compact_number(leg.theta),
                _format_compact_number(theta_usdt),
                _format_compact_number(leg.vega),
            )
            for column_index, value in enumerate(values):
                self._legs_table.setItem(row_index, column_index, QTableWidgetItem(str(value)))

    def _refresh_strategy_summary(self) -> None:
        if not self._legs:
            self._strategy_summary_label.setText("暂无策略腿。")
            return
        formula = self._formula_edit.text().strip() or build_default_formula(self._legs)
        aliases = {item.alias for item in self._legs if item.alias.strip()}
        combo_value = "-"
        try:
            latest_values: dict[str, Decimal | None] = {}
            for leg in self._legs:
                quote = self._quotes_by_inst_id.get(leg.inst_id)
                reference_value = quote.reference_price if quote is not None else leg.premium
                instrument = self._instrument_map.get(leg.inst_id)
                if reference_value is None or leg.premium is None:
                    latest_values[leg.alias] = None
                    continue
                latest_values[leg.alias] = build_option_pnl_value(
                    reference_value,
                    entry_price=leg.premium,
                    contract_value=option_contract_value(instrument) if instrument is not None else Decimal("1"),
                )
            if all(value is not None for value in latest_values.values()):
                combo_value = _format_compact_number(
                    evaluate_linear_formula(
                        formula,
                        {name: value for name, value in latest_values.items() if isinstance(value, Decimal)},
                        allowed_names=aliases,
                    )
                )
        except Exception:
            combo_value = "-"
        net_premium: Decimal | None = Decimal("0")
        premium_ccy: str | None = None
        for leg in self._legs:
            instrument = self._instrument_map.get(leg.inst_id)
            if instrument is None or leg.premium is None:
                net_premium = None
                break
            currency = instrument.ct_val_ccy or leg.inst_id.split("-", 1)[0]
            if premium_ccy is None:
                premium_ccy = currency
            elif premium_ccy != currency:
                net_premium = None
                break
            direction = Decimal("1") if leg.side == "buy" else Decimal("-1")
            premium_cost = leg.premium * option_contract_value(instrument) * leg.quantity
            net_premium += -direction * premium_cost
        premium_text = f"{_format_compact_number(net_premium)} {premium_ccy or ''}".strip() if net_premium is not None else "跨币种/待刷新"
        underlying_text = f" | 标的≈{_format_compact_number(self._current_underlying_price)}" if self._current_underlying_price else ""
        self._strategy_summary_label.setText(
            f"策略腿 {len(self._legs)} 条 | 净权利金 {premium_text} | 当前组合浮盈亏 {combo_value}{underlying_text}\n"
            f"组合公式 {formula or '-'}"
        )

    @Slot()
    def use_default_formula(self) -> None:
        self._formula_edit.setText(build_default_formula(self._legs))
        self._refresh_strategy_summary()

    @Slot()
    def refresh_leg_quotes(self) -> None:
        if not self._legs:
            QMessageBox.information(self, "刷新腿报价", "当前没有策略腿。")
            return
        self._status_label.setText("正在刷新策略腿报价...")
        thread = _LegQuoteThread(legs=self._legs, instrument_map=self._instrument_map, client=self._client, parent=self)
        thread.snapshot_ready.connect(self._apply_refreshed_leg_quotes)
        thread.error_raised.connect(lambda message: QMessageBox.critical(self, "刷新腿报价失败", message))
        self._start_thread("leg_quotes", thread)

    @Slot(object)
    def _apply_refreshed_leg_quotes(self, refreshed: object) -> None:
        if not isinstance(refreshed, tuple):
            return
        for inst_id, instrument, quote in refreshed:
            self._instrument_map[inst_id] = instrument
            self._quotes_by_inst_id[inst_id] = quote
            if self._current_underlying_price is None and quote.index_price is not None:
                self._current_underlying_price = quote.index_price
        if self._current_underlying_price is None:
            self._current_underlying_price = self._load_spot_reference_price_for_legs(self._legs)
        self._refresh_leg_greeks()
        self._render_legs()
        self._refresh_strategy_summary()
        self._status_label.setText("策略腿报价已刷新。")

    def _start_import_positions(self, *, scope: str) -> None:
        family = self._family_combo.currentText().strip().upper()
        expiry = self._selected_expiry_code()
        if not family:
            QMessageBox.critical(self, "导入持仓失败", "请先选择期权系列。")
            return
        if scope == "expiry" and not expiry:
            QMessageBox.critical(self, "导入持仓失败", "请先选择到期日。")
            return
        replace_existing = True
        if self._legs:
            box = QMessageBox(self)
            box.setWindowTitle("导入持仓")
            box.setText("当前已有策略腿。")
            box.setInformativeText("选择“是”清空后导入；选择“否”追加导入；选择“取消”放弃本次导入。")
            box.setStandardButtons(
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No | QMessageBox.StandardButton.Cancel
            )
            answer = box.exec()
            if answer == int(QMessageBox.StandardButton.Cancel):
                return
            replace_existing = answer == int(QMessageBox.StandardButton.Yes)
        self._position_import_request_id += 1
        request_id = self._position_import_request_id
        alias_start = 0 if replace_existing else self._alias_counter
        scope_text = "当前到期日" if scope == "expiry" else "当前系列"
        self._status_label.setText(f"正在导入{scope_text}持仓...")
        thread = _ImportPositionsThread(
            request_id=request_id,
            profile_name=self._profile_name,
            family=family,
            expiry=expiry,
            scope=scope,
            replace_existing=replace_existing,
            alias_start=alias_start,
            client=self._client,
            parent=self,
        )
        thread.snapshot_ready.connect(self._apply_imported_positions)
        thread.error_raised.connect(self._show_import_positions_error)
        self._start_thread("import_positions", thread)

    @Slot(int, object)
    def _apply_imported_positions(self, request_id: int, snapshot: object) -> None:
        if request_id != self._position_import_request_id:
            return
        if not isinstance(snapshot, ImportSnapshot):
            return
        old_default_formula = build_default_formula(self._legs) if self._legs else ""
        if snapshot.replace_existing:
            self._legs = []
            self.clear_legs()
        for leg, instrument, quote in snapshot.imported:
            self._legs.append(leg)
            self._instrument_map[instrument.inst_id] = instrument
            if quote is not None:
                self._quotes_by_inst_id[instrument.inst_id] = quote
                if self._current_underlying_price is None and quote.index_price is not None:
                    self._current_underlying_price = quote.index_price
        self._family_instruments_cache[snapshot.family] = list(snapshot.family_instruments)
        self._family_tickers_cache[snapshot.family] = dict(snapshot.tickers_by_inst_id)
        self._all_option_instruments = list(snapshot.family_instruments)
        for instrument in snapshot.family_instruments:
            self._instrument_map[instrument.inst_id] = instrument
        self._alias_counter = max(
            self._alias_counter,
            max((int(leg.alias[1:]) for leg, *_ in snapshot.imported if leg.alias.startswith("L") and leg.alias[1:].isdigit()), default=0),
        )
        if self._current_underlying_price is None:
            self._current_underlying_price = self._load_spot_reference_price_for_legs(self._legs)
        if not self._formula_edit.text().strip() or self._formula_edit.text().strip() == old_default_formula:
            self._formula_edit.setText(build_default_formula(self._legs))
        self._refresh_leg_greeks()
        self._render_legs()
        self._refresh_strategy_summary()
        scope_text = "当前到期日" if snapshot.scope == "expiry" else "当前系列"
        context = f"{snapshot.family} {snapshot.expiry}" if snapshot.scope == "expiry" and snapshot.expiry else snapshot.family
        self._status_label.setText(f"已导入 {scope_text} 持仓：{context} | {len(snapshot.imported)} 条策略腿")

    @Slot(int, str)
    def _show_import_positions_error(self, request_id: int, message: str) -> None:
        if request_id != self._position_import_request_id:
            return
        self._status_label.setText("导入持仓失败")
        QMessageBox.critical(self, "导入持仓失败", message)

    def _validate_chart_inputs(self) -> tuple[int, str] | None:
        if not self._legs:
            QMessageBox.information(self, "刷新图表", "请先至少加入一条策略腿。")
            return None
        try:
            candle_limit = self._parse_positive_int(self._candle_limit_edit.text(), "K线数量")
        except Exception as exc:  # noqa: BLE001
            QMessageBox.critical(self, "图表参数错误", str(exc))
            return None
        if candle_limit > MAX_OPTION_COMBO_CANDLES:
            QMessageBox.critical(self, "图表参数错误", f"组合 K 线当前最多支持 {MAX_OPTION_COMBO_CANDLES} 根标记价格 K 线。")
            return None
        aliases = {item.alias for item in self._legs if item.alias.strip()}
        formula = self._formula_edit.text().strip() or build_default_formula(self._legs)
        if not formula:
            QMessageBox.critical(self, "图表参数错误", "请先加入有效策略腿，再生成组合公式。")
            return None
        try:
            parse_linear_formula(formula, allowed_names=aliases)
        except Exception as exc:  # noqa: BLE001
            QMessageBox.critical(self, "组合公式错误", str(exc))
            return None
        return candle_limit, formula

    @Slot()
    def refresh_charts(self) -> None:
        validated = self._validate_chart_inputs()
        if validated is None:
            return
        candle_limit, formula = validated
        self._chart_request_id += 1
        request_id = self._chart_request_id
        self._status_label.setText("正在生成到期盈亏图和组合 K 线...")
        thread = _ChartThread(
            request_id=request_id,
            mode="all",
            legs=self._legs,
            instrument_map=self._instrument_map,
            client=self._client,
            bar=str(self._bar_combo.currentData() or self._bar_combo.currentText() or "1H"),
            candle_limit=candle_limit,
            formula=formula,
            current_underlying_price=self._current_underlying_price,
            parent=self,
        )
        thread.snapshot_ready.connect(self._apply_chart_snapshot)
        thread.error_raised.connect(self._show_chart_error)
        self._start_thread("chart", thread)

    @Slot()
    def refresh_combo_chart(self) -> None:
        validated = self._validate_chart_inputs()
        if validated is None:
            return
        candle_limit, formula = validated
        self._chart_request_id += 1
        request_id = self._chart_request_id
        self._status_label.setText("正在刷新组合 K 线...")
        thread = _ChartThread(
            request_id=request_id,
            mode="combo",
            legs=self._legs,
            instrument_map=self._instrument_map,
            client=self._client,
            bar=str(self._bar_combo.currentData() or self._bar_combo.currentText() or "1H"),
            candle_limit=candle_limit,
            formula=formula,
            current_underlying_price=self._current_underlying_price,
            parent=self,
        )
        thread.snapshot_ready.connect(self._apply_chart_snapshot)
        thread.error_raised.connect(self._show_chart_error)
        self._start_thread("chart", thread)

    @Slot(int, object)
    def _apply_chart_snapshot(self, request_id: int, snapshot: object) -> None:
        if request_id != self._chart_request_id:
            return
        if not isinstance(snapshot, ChartSnapshot):
            return
        self._latest_combo_candles = list(snapshot.combo_candles)
        self._latest_combo_requested_limit = snapshot.requested_limit
        self._latest_combo_source_counts = dict(snapshot.source_counts)
        self._latest_combo_value = snapshot.latest_combo_value
        self._latest_spot_usdt_price = snapshot.spot_usdt_price or snapshot.current_underlying_price
        self._latest_spot_usdt_candles = list(snapshot.spot_usdt_candles)
        self._latest_chart_formula = snapshot.formula
        self._current_underlying_price = snapshot.current_underlying_price
        for inst_id, quote in snapshot.latest_quotes.items():
            self._quotes_by_inst_id[inst_id] = quote
        if snapshot.payoff_snapshot is not None:
            self._latest_expiry_payoff_snapshot = snapshot.payoff_snapshot
            self._latest_payoff_snapshot = snapshot.payoff_snapshot
            self._latest_resolved_legs = list(snapshot.resolved_legs)
            self._latest_implied_volatility_by_alias = dict(snapshot.implied_volatility_by_alias)
            self._latest_payoff_loaded_at = snapshot.payoff_loaded_at
            self._latest_payoff_expiry_at = (
                max(parse_option_expiry_datetime(item.expiry_code) for item in snapshot.resolved_legs)
                if snapshot.resolved_legs
                else None
            )
        self._refresh_deribit_volatility_series(snapshot.requested_limit)
        self._refresh_leg_greeks()
        self._render_legs()
        self._refresh_strategy_summary()
        if snapshot.payoff_snapshot is not None:
            self._refresh_payoff_simulation()
        self._refresh_chart_display(combo_only=snapshot.payoff_snapshot is None)
        self._status_label.setText("期权策略图表已更新。" if snapshot.payoff_snapshot is not None else "组合 K 线已更新。")
        if self._big_dialog is not None and self._big_dialog.isVisible():
            self._refresh_big_chart_window()

    @Slot(int, str)
    def _show_chart_error(self, request_id: int, message: str) -> None:
        if request_id != self._chart_request_id:
            return
        self._status_label.setText("图表生成失败")
        QMessageBox.critical(self, "图表生成失败", message)

    def _refresh_deribit_volatility_series(self, requested_limit: int) -> None:
        currency = self._current_deribit_currency()
        if not currency:
            self._latest_deribit_volatility_candles = []
            self._latest_deribit_resolution_label = ""
            self._latest_deribit_resolution_note = ""
            return
        candles, resolution_label, resolution_note = _load_deribit_option_chart_candles(
            currency,
            bar=str(self._bar_combo.currentData() or self._bar_combo.currentText() or "1H"),
            requested_limit=requested_limit,
        )
        self._latest_deribit_volatility_candles = candles
        self._latest_deribit_resolution_label = resolution_label
        self._latest_deribit_resolution_note = resolution_note

    def _current_deribit_currency(self) -> str | None:
        if self._latest_resolved_legs:
            return parse_option_contract(self._latest_resolved_legs[0].inst_id).inst_family.split("-", 1)[0]
        if self._legs:
            return parse_option_contract(self._legs[0].inst_id).inst_family.split("-", 1)[0]
        family_text = self._family_combo.currentText().strip().upper()
        return family_text.split("-", 1)[0] if family_text else None

    def _display_in_usdt(self) -> bool:
        return str(self._display_ccy_combo.currentData() or self._display_ccy_combo.currentText()).strip().upper() == "USDT"

    def _combo_display_in_usdt(self) -> bool:
        return str(self._combo_ccy_combo.currentData() or self._combo_ccy_combo.currentText()).strip().upper() == "USDT"

    def _payoff_snapshot_for_display(self, snapshot: StrategyPayoffSnapshot) -> tuple[StrategyPayoffSnapshot, str]:
        if not self._display_in_usdt():
            return snapshot, _native_display_currency(self._legs, self._instrument_map)
        reference_price = self._latest_spot_usdt_price or snapshot.current_underlying_price
        return convert_payoff_snapshot_to_usdt(snapshot, reference_price=reference_price), "USDT"

    def _combo_candles_for_display(self, candles: list[Candle]) -> tuple[list[Candle], str, bool]:
        native_ccy = _native_display_currency(self._legs, self._instrument_map)
        if not self._combo_display_in_usdt():
            return candles, native_ccy, True
        if not self._latest_spot_usdt_candles:
            return candles, native_ccy, False
        converted = convert_candles_by_reference(candles, self._latest_spot_usdt_candles)
        if not converted:
            return candles, native_ccy, False
        return converted, "USDT", True

    def _current_payoff_valuation_time(self) -> datetime | None:
        if self._latest_payoff_loaded_at is None or self._latest_payoff_expiry_at is None:
            return None
        start_time = self._latest_payoff_loaded_at
        end_time = self._latest_payoff_expiry_at
        if end_time <= start_time:
            return end_time
        progress = max(0.0, min(float(self._payoff_time_slider.value()), 100.0)) / 100.0
        return start_time + ((end_time - start_time) * progress)

    def _current_volatility_shift_decimal(self) -> Decimal:
        return Decimal(str(self._payoff_vol_slider.value() / 100.0))

    def _reset_payoff_simulation_controls(self) -> None:
        self._payoff_time_slider.setValue(100)
        self._payoff_vol_slider.setValue(0)
        self._payoff_sim_date_label.setText("估值日 -")
        self._payoff_vol_shift_label.setText("波动率平移 0%")

    @Slot()
    def _on_payoff_slider_changed(self) -> None:
        self._update_payoff_simulation_labels()
        self._refresh_payoff_simulation()

    def _update_payoff_simulation_labels(self) -> None:
        valuation_time = self._current_payoff_valuation_time()
        progress = self._payoff_time_slider.value()
        if valuation_time is None:
            self._payoff_sim_date_label.setText("估值日 -")
        else:
            self._payoff_sim_date_label.setText(f"估值日 {valuation_time.strftime('%Y-%m-%d')} | 时间进度 {progress}%")
        self._payoff_vol_shift_label.setText(f"波动率平移 {_format_signed_percent(Decimal(str(self._payoff_vol_slider.value())))}")

    def _payoff_chart_mode_label(self) -> str:
        progress = self._payoff_time_slider.value()
        vol_shift = abs(self._payoff_vol_slider.value())
        if progress >= 100 and vol_shift <= 0:
            return "到期盈亏"
        valuation_time = self._current_payoff_valuation_time()
        if valuation_time is not None and self._latest_payoff_loaded_at is not None and valuation_time.date() == self._latest_payoff_loaded_at.date():
            return "当日模拟盈亏"
        return "模拟盈亏"

    def _refresh_payoff_simulation(self) -> None:
        if not self._latest_resolved_legs or self._latest_payoff_loaded_at is None:
            self._update_payoff_simulation_labels()
            return
        valuation_time = self._current_payoff_valuation_time()
        if valuation_time is None:
            return
        snapshot = build_simulated_payoff_snapshot(
            self._latest_resolved_legs,
            implied_volatility_by_alias=self._latest_implied_volatility_by_alias,
            valuation_time=valuation_time,
            volatility_shift=self._current_volatility_shift_decimal(),
            current_underlying_price=self._current_underlying_price,
        )
        self._latest_payoff_snapshot = snapshot
        self._update_payoff_simulation_labels()
        self._refresh_chart_display()

    @Slot()
    def _refresh_chart_display(self, combo_only: bool = False) -> None:
        formula = self._latest_chart_formula or self._formula_edit.text().strip() or build_default_formula(self._legs)
        if not combo_only and self._latest_payoff_snapshot is not None:
            mode_label = self._payoff_chart_mode_label()
            payoff_snapshot, payoff_ccy = self._payoff_snapshot_for_display(self._latest_payoff_snapshot)
            reference_snapshot: StrategyPayoffSnapshot | None = None
            if mode_label != "到期盈亏" and self._latest_expiry_payoff_snapshot is not None and self._latest_expiry_payoff_snapshot.points:
                reference_snapshot, _ = self._payoff_snapshot_for_display(self._latest_expiry_payoff_snapshot)
            break_even_text = " / ".join(_format_compact_number(item) for item in self._latest_payoff_snapshot.break_even_prices) if self._latest_payoff_snapshot.break_even_prices else "无"
            underlying_text = f"当前标的≈{_format_compact_number(self._current_underlying_price)}" if self._current_underlying_price is not None else "当前标的指数暂不可用"
            valuation_time = self._current_payoff_valuation_time()
            valuation_text = valuation_time.strftime("%Y-%m-%d") if valuation_time is not None else "-"
            compare_text = " | 叠加到期盈亏对比" if reference_snapshot is not None else ""
            self._payoff_summary_label.setText(
                f"{underlying_text} | 单位 {payoff_ccy} | 估值日 {valuation_text} | 波动率平移 {_format_signed_percent(self._current_volatility_shift_decimal() * Decimal('100'))}\n"
                f"净权利金 {_format_compact_number(payoff_snapshot.net_premium)} | 盈亏平衡点 {break_even_text}{compare_text}"
            )
            self._payoff_chart.set_snapshot(
                payoff_snapshot,
                value_ccy=payoff_ccy,
                mode_label=mode_label,
                reference_snapshot=reference_snapshot,
            )
        if self._latest_combo_candles:
            combo_candles, combo_ccy, converted = self._combo_candles_for_display(self._latest_combo_candles)
            latest_candle = combo_candles[-1] if combo_candles else None
            latest_value_text = _format_compact_number(latest_candle.close) if latest_candle is not None else _format_compact_number(self._latest_combo_value)
            latest_candle_text = (
                f"O {_format_compact_number(latest_candle.open)} / H {_format_compact_number(latest_candle.high)} / "
                f"L {_format_compact_number(latest_candle.low)} / C {_format_compact_number(latest_candle.close)}"
                if latest_candle is not None
                else "暂无组合浮盈亏 K 线"
            )
            note = ""
            if self._combo_display_in_usdt() and not converted:
                note = f" | 缺少 {_native_display_currency(self._legs, self._instrument_map)}-USDT 历史，当前按结算币显示"
            alignment_note = ""
            if self._latest_combo_requested_limit is not None:
                requested_text = f"请求 {self._latest_combo_requested_limit} 根"
                actual_text = f"实际 {len(combo_candles)} 根"
                counts_text = " / ".join(f"{alias}={count}" for alias, count in sorted(self._latest_combo_source_counts.items()))
                alignment_note = f" | 多腿共同时间对齐（{requested_text}，{actual_text}；各腿 {counts_text}）" if counts_text else f" | {requested_text}，{actual_text}"
            self._combo_summary_label.setText(
                f"公式: {formula}\n周期 {self._bar_combo.currentText()} | 根数 {len(combo_candles)} | 单位 {combo_ccy} | 最新组合浮盈亏 {latest_value_text} | {latest_candle_text} | 口径 (标记价-持仓价)*张数*每张面值{note}{alignment_note}"
            )
            self._combo_chart.set_candles(
                title=f"组合浮盈亏K线 ({combo_ccy})",
                candles=combo_candles,
                hide_wicks=self._hide_wicks_check.isChecked(),
            )
        if self._big_dialog is not None and self._big_dialog.isVisible():
            self._refresh_big_chart_window()

    def _load_spot_reference_price_for_legs(self, active_legs: list[StrategyLegDefinition]) -> Decimal | None:
        families = {parse_option_contract(item.inst_id).inst_family for item in active_legs}
        if len(families) != 1:
            return None
        spot_inst_id = _spot_usdt_inst_id(next(iter(families)))
        if not spot_inst_id:
            return None
        try:
            spot_ticker = self._client.get_ticker(spot_inst_id)
        except Exception:
            return None
        return spot_ticker.last or spot_ticker.bid or spot_ticker.ask

    @Slot()
    def save_current_strategy(self) -> None:
        name = self._strategy_name_edit.text().strip()
        if not name:
            QMessageBox.critical(self, "保存策略失败", "请先填写策略名称。")
            return
        if not self._legs:
            QMessageBox.critical(self, "保存策略失败", "当前没有可保存的策略腿。")
            return
        records = list(self._saved_strategies)
        existing_index = next((index for index, item in enumerate(records) if str(item.get("name", "")).strip() == name), None)
        if existing_index is not None:
            answer = QMessageBox.question(self, "保存策略", f"策略 {name} 已存在，是否覆盖？")
            if answer != QMessageBox.StandardButton.Yes:
                return
        payload = {
            "name": name,
            "option_family": self._family_combo.currentText().strip().upper(),
            "expiry_code": self._selected_expiry_code(),
            "bar": self._bar_combo.currentText().strip(),
            "candle_limit": self._candle_limit_edit.text().strip(),
            "chart_display_ccy": str(self._display_ccy_combo.currentData() or "结算币"),
            "formula": self._formula_edit.text().strip(),
            "legs": [
                {
                    "alias": item.alias,
                    "inst_id": item.inst_id,
                    "side": item.side,
                    "quantity": format_decimal(item.quantity),
                    "premium": format_decimal(item.premium) if item.premium is not None else "",
                    "delta": format_decimal(item.delta) if item.delta is not None else "",
                    "gamma": format_decimal(item.gamma) if item.gamma is not None else "",
                    "theta": format_decimal(item.theta) if item.theta is not None else "",
                    "vega": format_decimal(item.vega) if item.vega is not None else "",
                    "enabled": item.enabled,
                }
                for item in self._legs
            ],
        }
        if existing_index is not None:
            records[existing_index] = payload
        else:
            records.append(payload)
        save_option_strategies_snapshot(records)
        self._saved_strategies = records
        self._refresh_saved_strategy_options()
        self._saved_strategy_combo.setCurrentText(name)
        self._status_label.setText(f"策略 {name} 已保存。")

    @Slot()
    def load_selected_strategy(self) -> None:
        name = self._saved_strategy_combo.currentText().strip()
        if not name:
            QMessageBox.information(self, "加载策略", "请先从已保存策略里选择一个名称。")
            return
        record = next((item for item in self._saved_strategies if str(item.get("name", "")).strip() == name), None)
        if record is None:
            QMessageBox.critical(self, "加载策略失败", "没有找到对应的策略记录。")
            return
        try:
            self._apply_saved_strategy(record)
        except Exception as exc:  # noqa: BLE001
            QMessageBox.critical(self, "加载策略失败", str(exc))
            return
        self._status_label.setText(f"策略 {name} 已加载。")

    @Slot()
    def delete_selected_strategy(self) -> None:
        name = self._saved_strategy_combo.currentText().strip()
        if not name:
            QMessageBox.information(self, "删除策略", "请先从已保存策略里选择一个名称。")
            return
        answer = QMessageBox.question(self, "删除策略", f"确定删除策略 {name} 吗？")
        if answer != QMessageBox.StandardButton.Yes:
            return
        records = [item for item in self._saved_strategies if str(item.get("name", "")).strip() != name]
        save_option_strategies_snapshot(records)
        self._saved_strategies = records
        if self._strategy_name_edit.text().strip() == name:
            self._strategy_name_edit.clear()
        self._refresh_saved_strategy_options()
        self._status_label.setText(f"策略 {name} 已删除。")

    def _apply_saved_strategy(self, record: dict[str, object]) -> None:
        self._strategy_name_edit.setText(str(record.get("name", "")))
        family = str(record.get("option_family", "")).strip().upper()
        self._family_combo.setCurrentText(family)
        self._sync_expiry_options(preferred=str(record.get("expiry_code", "")).strip())
        bar = str(record.get("bar", "1H")).strip() or "1H"
        self._bar_combo.setCurrentText(bar)
        self._candle_limit_edit.setText(str(record.get("candle_limit", "1000")).strip() or "1000")
        display_ccy = str(record.get("chart_display_ccy", "结算币")).strip() or "结算币"
        self._display_ccy_combo.setCurrentText(display_ccy)
        self._combo_ccy_combo.setCurrentText(display_ccy)
        self._formula_edit.setText(str(record.get("formula", "")).strip())
        raw_legs = record.get("legs", [])
        if not isinstance(raw_legs, list):
            raise ValueError("策略腿数据格式无效。")
        restored: list[StrategyLegDefinition] = []
        max_alias_index = 0
        for raw in raw_legs:
            if not isinstance(raw, dict):
                continue
            alias = str(raw.get("alias", "")).strip()
            inst_id = str(raw.get("inst_id", "")).strip().upper()
            side = str(raw.get("side", "buy")).strip().lower()
            enabled = bool(raw.get("enabled", True))
            quantity = self._parse_positive_decimal(str(raw.get("quantity", "1")), "策略腿数量")
            premium_text = str(raw.get("premium", "")).strip()
            premium = Decimal(premium_text) if premium_text else None
            delta_text = str(raw.get("delta", "")).strip()
            gamma_text = str(raw.get("gamma", "")).strip()
            theta_text = str(raw.get("theta", "")).strip()
            vega_text = str(raw.get("vega", "")).strip()
            if not alias or not inst_id or side not in {"buy", "sell"}:
                continue
            restored.append(
                StrategyLegDefinition(
                    alias=alias,
                    inst_id=inst_id,
                    side="buy" if side == "buy" else "sell",
                    quantity=quantity,
                    premium=premium,
                    delta=Decimal(delta_text) if delta_text else None,
                    gamma=Decimal(gamma_text) if gamma_text else None,
                    theta=Decimal(theta_text) if theta_text else None,
                    vega=Decimal(vega_text) if vega_text else None,
                    enabled=enabled,
                )
            )
            if alias.startswith("L") and alias[1:].isdigit():
                max_alias_index = max(max_alias_index, int(alias[1:]))
        if not restored:
            raise ValueError("策略里没有可用的策略腿。")
        self._alias_counter = max(self._alias_counter, max_alias_index)
        self._legs = restored
        for leg in restored:
            if leg.inst_id in self._instrument_map:
                continue
            try:
                self._instrument_map[leg.inst_id] = self._client.get_instrument(leg.inst_id)
            except Exception:
                pass
        self._refresh_leg_greeks()
        self._render_legs()
        self._refresh_strategy_summary()
        if family and self._selected_expiry_code():
            self.refresh_chain()
        self.refresh_charts()

    @Slot()
    def open_big_chart_window(self) -> None:
        if self._big_dialog is None:
            self._big_dialog = OptionStrategyBigChartDialog(self)
            self._big_dialog._overlay_refresh_button.clicked.connect(self._request_overlay_chart_refresh)
            self._big_dialog._overlay_period_combo.currentIndexChanged.connect(self._request_overlay_chart_refresh)
            self._big_dialog._overlay_combo_chart.hover_changed.connect(self._sync_overlay_chart_hover)
            self._big_dialog._overlay_vol_chart.hover_changed.connect(self._sync_overlay_chart_hover)
            self._big_dialog._overlay_spot_chart.hover_changed.connect(self._sync_overlay_chart_hover)
            self._big_dialog._overlay_combo_chart.hover_cleared.connect(self._clear_overlay_chart_hover)
            self._big_dialog._overlay_vol_chart.hover_cleared.connect(self._clear_overlay_chart_hover)
            self._big_dialog._overlay_spot_chart.hover_cleared.connect(self._clear_overlay_chart_hover)
        self._big_dialog.show()
        self._big_dialog.raise_()
        self._big_dialog.activateWindow()
        self._refresh_big_chart_window()

    def _refresh_big_chart_window(self) -> None:
        dialog = self._big_dialog
        if dialog is None:
            return
        dialog._payoff_note.setText(self._payoff_summary_label.text())
        dialog._combo_note.setText(self._combo_summary_label.text())
        if self._latest_payoff_snapshot is not None:
            mode_label = self._payoff_chart_mode_label()
            payoff_snapshot, payoff_ccy = self._payoff_snapshot_for_display(self._latest_payoff_snapshot)
            reference_snapshot = None
            if mode_label != "到期盈亏" and self._latest_expiry_payoff_snapshot is not None and self._latest_expiry_payoff_snapshot.points:
                reference_snapshot, _ = self._payoff_snapshot_for_display(self._latest_expiry_payoff_snapshot)
            dialog._payoff_chart.set_snapshot(
                payoff_snapshot,
                value_ccy=payoff_ccy,
                mode_label=mode_label,
                reference_snapshot=reference_snapshot,
            )
        else:
            dialog._payoff_chart.show_message("加入策略腿后，可生成到期盈亏图。")
        if self._latest_combo_candles:
            combo_candles, combo_ccy, _converted = self._combo_candles_for_display(self._latest_combo_candles)
            dialog._combo_chart.set_candles(
                title=f"组合浮盈亏K线 ({combo_ccy})",
                candles=combo_candles,
                hide_wicks=self._hide_wicks_check.isChecked(),
            )
        else:
            dialog._combo_chart.show_message("组合浮盈亏 K 线按持仓价差计算；先加入策略腿再生成。")
        currency = self._current_deribit_currency() or "—"
        if self._latest_deribit_volatility_candles:
            note_text = f" | {self._latest_deribit_resolution_note}" if self._latest_deribit_resolution_note else ""
            dialog._vol_note.setText(
                f"Deribit {currency} DVOL 波动率指数 K 线 | 周期 {self._latest_deribit_resolution_label}{note_text} | 根数 {len(self._latest_deribit_volatility_candles)}"
            )
            dialog._vol_chart.set_candles(
                title=f"Deribit {currency} DVOL 波动率指数K线 (%)",
                candles=self._latest_deribit_volatility_candles,
            )
        else:
            dialog._vol_note.setText(f"Deribit {currency} DVOL 暂无可用缓存；请先在“Deribit 波动率指数”窗口刷新该币种数据。")
            dialog._vol_chart.show_message("暂无可用的 Deribit 波动率指数 K 线数据。")
        if self._overlay_triples:
            dialog._overlay_note.setText(
                f"叠加对比 | 上=组合浮盈亏K线({self._overlay_combo_ccy}) | 中=Deribit {self._overlay_vol_currency} DVOL (%) | 下={self._overlay_spot_inst_id} 现货 | "
                f"共用时间轴 | 周期 {self._overlay_resolution_label}" + (f" | {self._overlay_resolution_note}" if self._overlay_resolution_note else "")
            )
            dialog._overlay_combo_chart.set_candles(title=f"组合浮盈亏K线 ({self._overlay_combo_ccy})", candles=[item[0] for item in self._overlay_triples], hide_wicks=self._hide_wicks_check.isChecked())
            dialog._overlay_vol_chart.set_candles(title=f"Deribit {self._overlay_vol_currency} DVOL (%)", candles=[item[1] for item in self._overlay_triples])
            dialog._overlay_spot_chart.set_candles(title=f"{self._overlay_spot_inst_id} 现货K线", candles=[item[2] for item in self._overlay_triples])
        else:
            dialog._overlay_note.setText("叠加对比：请先加入策略腿并刷新图表后，再按需要刷新叠加对比。")
            dialog._overlay_combo_chart.show_message("请先刷新叠加对比。")
            dialog._overlay_vol_chart.show_message("请先刷新叠加对比。")
            dialog._overlay_spot_chart.show_message("请先刷新叠加对比。")
        self._clear_overlay_chart_hover()

    @Slot(int, float)
    def _sync_overlay_chart_hover(self, index: int, y_ratio: float) -> None:
        dialog = self._big_dialog
        if dialog is None:
            return
        self._overlay_hover_index = index
        self._overlay_hover_y_ratio = y_ratio
        dialog._overlay_combo_chart.set_linked_hover(index, y_ratio)
        dialog._overlay_vol_chart.set_linked_hover(index, y_ratio)
        dialog._overlay_spot_chart.set_linked_hover(index, y_ratio)

    @Slot()
    def _clear_overlay_chart_hover(self) -> None:
        dialog = self._big_dialog
        self._overlay_hover_index = None
        self._overlay_hover_y_ratio = None
        if dialog is None:
            return
        dialog._overlay_combo_chart.set_linked_hover(None, None)
        dialog._overlay_vol_chart.set_linked_hover(None, None)
        dialog._overlay_spot_chart.set_linked_hover(None, None)

    @Slot()
    def _request_overlay_chart_refresh(self) -> None:
        if self._big_dialog is None or not self._legs:
            return
        validated = self._validate_chart_inputs()
        if validated is None:
            return
        candle_limit, formula = validated
        self._overlay_chart_request_id += 1
        request_id = self._overlay_chart_request_id
        bar = self._big_dialog.overlay_period
        self._big_dialog._overlay_note.setText(f"正在加载叠加对比（{bar}）…")
        thread = _OverlayThread(
            request_id=request_id,
            legs=self._legs,
            instrument_map=self._instrument_map,
            client=self._client,
            bar=bar,
            candle_limit=candle_limit,
            formula=formula,
            display_in_usdt=self._combo_display_in_usdt(),
            parent=self,
        )
        thread.snapshot_ready.connect(self._apply_overlay_snapshot)
        thread.error_raised.connect(self._show_overlay_error)
        self._start_thread("overlay", thread)

    @Slot(int, object)
    def _apply_overlay_snapshot(self, request_id: int, snapshot: object) -> None:
        if request_id != self._overlay_chart_request_id:
            return
        if not isinstance(snapshot, OverlaySnapshot):
            return
        self._overlay_triples = list(snapshot.triples)
        self._overlay_combo_ccy = snapshot.combo_ccy
        self._overlay_spot_inst_id = snapshot.spot_inst_id
        self._overlay_vol_currency = snapshot.vol_currency
        self._overlay_resolution_label = snapshot.resolution_label
        self._overlay_resolution_note = snapshot.resolution_note
        self._refresh_big_chart_window()

    @Slot(int, str)
    def _show_overlay_error(self, request_id: int, message: str) -> None:
        if request_id != self._overlay_chart_request_id:
            return
        self._overlay_triples = []
        if self._big_dialog is not None:
            self._big_dialog._overlay_note.setText(f"叠加对比加载失败：{message}")
            self._refresh_big_chart_window()
