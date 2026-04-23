from __future__ import annotations

import csv
import json
import threading
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal
from html import escape
from pathlib import Path
from typing import Callable, Literal

from okx_quant.enhanced_models import ChildSignalConfig
from okx_quant.enhanced_registry import EnhancedStrategyRegistry
from okx_quant.enhanced_runtime_config import (
    apply_strategy_runtime_payload,
    get_strategy_runtime_payload,
    load_runtime_store,
)
from okx_quant.enhanced_seed_strategies import (
    PARENT_STRATEGY_ID,
    PARENT_STRATEGY_NAME,
    register_seed_strategy_package,
)
from okx_quant.enhanced_signal_engine import bar_to_minutes
from okx_quant.models import Candle, Credentials, StrategyConfig
from okx_quant.notifications import EmailNotifier
from okx_quant.okx_client import OkxFillHistoryItem, OkxPosition, OkxRestClient, infer_inst_type
from okx_quant.persistence import enhanced_strategy_runtime_file_path, live_strategy_sessions_dir_path
from okx_quant.pricing import format_decimal, snap_to_increment
from okx_quant.strategy_catalog import STRATEGY_SPOT_ENHANCEMENT_36_ID


Logger = Callable[[str], None]
ZERO = Decimal("0")
DEFAULT_SIGNAL_LOOKBACK = 256
DEFAULT_STATE_FLUSH_SECONDS = 12.0
DEFAULT_FILL_HISTORY_LIMIT = 200
SUPPORTED_PLAYBOOK_ACTIONS = {"SWAP_LONG", "SWAP_SHORT"}


@dataclass
class DirectionQuota:
    total: Decimal
    used: Decimal = ZERO

    @property
    def available(self) -> Decimal:
        return max(self.total - self.used, ZERO)


@dataclass
class LiveEnhancedPosition:
    position_id: str
    signal_id: str
    signal_name: str
    playbook_id: str
    playbook_name: str
    playbook_action: str
    direction: Literal["long", "short"]
    source_inst_id: str
    source_bar: str
    trade_inst_id: str
    quantity: Decimal
    signal_price: Decimal
    entry_price: Decimal
    entry_ts: int
    stop_loss_price: Decimal | None
    take_profit_price: Decimal | None
    max_hold_bars: int
    fee_rate: Decimal
    slippage_rate: Decimal
    trigger_reason: str
    pos_side: Literal["long", "short"] | None
    entry_order_id: str | None = None
    entry_client_order_id: str | None = None
    last_reference_price: Decimal | None = None
    last_reference_ts: int | None = None
    last_evaluated_candle_ts: int | None = None
    status: Literal["opened_auto", "manual_managed"] = "opened_auto"
    handoff_ts: int | None = None
    handoff_reason: str = ""

    @property
    def manual_pool(self) -> Literal["auto", "manual"]:
        return "manual" if self.status == "manual_managed" else "auto"

    @property
    def close_side(self) -> Literal["buy", "sell"]:
        return "sell" if self.direction == "long" else "buy"


@dataclass(frozen=True)
class CloseAllocation:
    position_id: str
    closed_qty: Decimal


@dataclass(frozen=True)
class ArtifactPaths:
    root_dir: Path
    state_json: Path
    manifest_json: Path
    active_csv: Path
    manual_csv: Path
    manual_html: Path
    events_jsonl: Path


@dataclass
class _EngineState:
    started_at: datetime
    artifacts: ArtifactPaths
    registry: EnhancedStrategyRegistry
    trade_inst_id: str
    signal_inst_id: str
    slot_size: Decimal
    max_slots_per_direction: int
    long_quota: DirectionQuota
    short_quota: DirectionQuota
    enabled_signal_ids: tuple[str, ...]
    active_positions: dict[str, LiveEnhancedPosition] = field(default_factory=dict)
    processed_signal_keys: set[str] = field(default_factory=set)
    seen_fill_keys: set[str] = field(default_factory=set)
    engine_order_ids: set[str] = field(default_factory=set)
    engine_client_order_ids: set[str] = field(default_factory=set)
    recent_events: list[dict[str, object]] = field(default_factory=list)
    closed_position_count: int = 0
    event_count: int = 0
    last_state_flush_at: datetime | None = None
    events_dirty: bool = False


def is_spot_enhancement_strategy_id(strategy_id: str) -> bool:
    return strategy_id == STRATEGY_SPOT_ENHANCEMENT_36_ID


def derive_spot_signal_inst_id(raw_inst_id: str) -> str:
    normalized = raw_inst_id.strip().upper()
    if not normalized:
        return ""
    if normalized.endswith("-SWAP"):
        parts = normalized.split("-")
        if len(parts) >= 3:
            return f"{parts[0]}-{parts[1]}"
    return normalized


def derive_swap_trade_inst_id(raw_inst_id: str) -> str:
    normalized = raw_inst_id.strip().upper()
    if not normalized:
        return ""
    if normalized.endswith("-SWAP"):
        return normalized
    if "-" in normalized:
        return f"{normalized}-SWAP"
    if normalized.endswith("USDT") and len(normalized) > 4:
        return f"{normalized[:-4]}-USDT-SWAP"
    return normalized


def apply_external_fill_reduction(
    positions: list[LiveEnhancedPosition],
    *,
    quantity: Decimal,
    direction: Literal["long", "short"],
) -> tuple[list[LiveEnhancedPosition], tuple[CloseAllocation, ...]]:
    remaining = max(quantity, ZERO)
    if remaining <= 0:
        return positions, ()
    ordered = sorted(
        (item for item in positions if item.direction == direction and item.quantity > 0),
        key=lambda item: (0 if item.status == "manual_managed" else 1, item.entry_ts, item.position_id),
    )
    by_id = {item.position_id: item for item in positions}
    allocations: list[CloseAllocation] = []
    for position in ordered:
        if remaining <= 0:
            break
        close_qty = min(position.quantity, remaining)
        if close_qty <= 0:
            continue
        updated_qty = max(position.quantity - close_qty, ZERO)
        if updated_qty <= 0:
            by_id.pop(position.position_id, None)
        else:
            by_id[position.position_id] = replace(position, quantity=updated_qty)
        allocations.append(CloseAllocation(position_id=position.position_id, closed_qty=close_qty))
        remaining -= close_qty
    survivors: list[LiveEnhancedPosition] = []
    for item in positions:
        updated = by_id.get(item.position_id)
        if updated is not None and updated.quantity > 0:
            survivors.append(updated)
    return survivors, tuple(allocations)


def _safe_token(value: str) -> str:
    cleaned = "".join(ch if ch.isascii() and (ch.isalnum() or ch in "._-") else "_" for ch in value)
    return cleaned.strip("._-") or "session"


def build_live_report_artifact_paths(
    *,
    started_at: datetime,
    session_id: str,
    trade_inst_id: str,
    root_dir: Path | None = None,
) -> ArtifactPaths:
    if root_dir is None:
        token = started_at.strftime("%Y%m%d_%H%M%S")
        root_dir = live_strategy_sessions_dir_path() / started_at.strftime("%Y-%m-%d") / (
            f"{token}__{_safe_token(session_id)}__{_safe_token(trade_inst_id)}"
        )
    else:
        root_dir = Path(root_dir)
    return ArtifactPaths(
        root_dir=root_dir,
        state_json=root_dir / "state.json",
        manifest_json=root_dir / "manifest.json",
        active_csv=root_dir / "active_positions.csv",
        manual_csv=root_dir / "manual_positions.csv",
        manual_html=root_dir / "manual_pool_dashboard.html",
        events_jsonl=root_dir / "events.jsonl",
    )


def _direction_quota(state: _EngineState, direction: Literal["long", "short"]) -> DirectionQuota:
    return state.long_quota if direction == "long" else state.short_quota


def _reserve_quota(state: _EngineState, direction: Literal["long", "short"], quantity: Decimal) -> bool:
    quota = _direction_quota(state, direction)
    if quota.available < quantity:
        return False
    quota.used += quantity
    return True


def _release_quota(state: _EngineState, direction: Literal["long", "short"], quantity: Decimal) -> None:
    quota = _direction_quota(state, direction)
    quota.used = max(quota.used - quantity, ZERO)


def _filter_enabled_signals(registry: EnhancedStrategyRegistry, signal_mode: str) -> list[ChildSignalConfig]:
    allowed: list[ChildSignalConfig] = []
    for item in registry.list_child_signals(PARENT_STRATEGY_ID, enabled_only=True):
        if signal_mode == "long_only" and item.direction_bias == "short":
            continue
        if signal_mode == "short_only" and item.direction_bias == "long":
            continue
        allowed.append(item)
    return allowed


def _signal_profile(
    registry: EnhancedStrategyRegistry,
    signal_id: str,
) -> dict[str, Decimal | int]:
    profile = registry.get_signal_lab_profile(signal_id)
    stop_loss_pct = Decimal("0.005")
    take_profit_pct = Decimal("0.010")
    max_hold_bars = 18
    fee_rate = ZERO
    slippage_rate = ZERO
    if profile is not None:
        if profile.stop_loss_pct is not None:
            stop_loss_pct = profile.stop_loss_pct
        if profile.take_profit_pct is not None:
            take_profit_pct = profile.take_profit_pct
        if profile.max_hold_bars is not None:
            max_hold_bars = profile.max_hold_bars
        if profile.fee_rate is not None:
            fee_rate = profile.fee_rate
        if profile.slippage_rate is not None:
            slippage_rate = profile.slippage_rate
    return {
        "stop_loss_pct": stop_loss_pct,
        "take_profit_pct": take_profit_pct,
        "max_hold_bars": max_hold_bars,
        "fee_rate": fee_rate,
        "slippage_rate": slippage_rate,
    }


def _fill_key(fill: OkxFillHistoryItem) -> str:
    return "|".join(
        [
            str(fill.order_id or ""),
            str(fill.trade_id or ""),
            str(fill.fill_time or ""),
            str(fill.inst_id or ""),
            str(fill.side or ""),
            str(fill.fill_size or ""),
        ]
    )


def _manual_fill_close_direction(fill: OkxFillHistoryItem) -> Literal["long", "short"] | None:
    side = (fill.side or "").lower()
    pos_side = (fill.pos_side or "").lower()
    if side == "sell" and pos_side in {"", "net", "long"}:
        return "long"
    if side == "buy" and pos_side in {"", "net", "short"}:
        return "short"
    return None


def _position_direction(position: OkxPosition) -> Literal["long", "short"] | None:
    if position.pos_side.lower() == "long":
        return "long"
    if position.pos_side.lower() == "short":
        return "short"
    if position.position > 0:
        return "long"
    if position.position < 0:
        return "short"
    return None


def _ts_from_position(position: OkxPosition) -> int:
    for key in ("uTime", "cTime", "ts"):
        value = position.raw.get(key)
        if value not in {None, ""}:
            try:
                return int(value)
            except Exception:
                continue
    return _now_ms()


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _holding_bars(entry_ts: int, reference_ts: int, bar: str) -> int:
    if reference_ts <= entry_ts:
        return 0
    return int((reference_ts - entry_ts) // (bar_to_minutes(bar) * 60 * 1000))


def _directional_points(
    *,
    entry_price: Decimal,
    exit_price: Decimal,
    direction: Literal["long", "short"],
) -> Decimal:
    return exit_price - entry_price if direction == "long" else entry_price - exit_price


def _directional_pnl(position: LiveEnhancedPosition, reference_price: Decimal) -> Decimal:
    return _directional_points(
        entry_price=position.entry_price,
        exit_price=reference_price,
        direction=position.direction,
    ) * position.quantity


def _stop_price(direction: Literal["long", "short"], entry_price: Decimal, pct: Decimal) -> Decimal | None:
    if entry_price <= 0 or pct <= 0:
        return None
    return entry_price * (Decimal("1") - pct) if direction == "long" else entry_price * (Decimal("1") + pct)


def _take_price(direction: Literal["long", "short"], entry_price: Decimal, pct: Decimal) -> Decimal | None:
    if entry_price <= 0 or pct <= 0:
        return None
    return entry_price * (Decimal("1") + pct) if direction == "long" else entry_price * (Decimal("1") - pct)


def _target_hit(
    *,
    direction: Literal["long", "short"],
    current_price: Decimal,
    target_price: Decimal,
    kind: Literal["tp", "sl"],
) -> bool:
    if direction == "long":
        return current_price >= target_price if kind == "tp" else current_price <= target_price
    return current_price <= target_price if kind == "tp" else current_price >= target_price


def _serialize_value(value: object) -> object:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _serialize_value(item) for key, item in value.items()}
    return value


def _deserialize_decimal(value: object, default: Decimal = ZERO) -> Decimal:
    if value in {None, ""}:
        return default
    try:
        return Decimal(str(value))
    except Exception:
        return default


def _deserialize_optional_decimal(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _deserialize_optional_int(value: object) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    fieldnames = list(rows[0].keys()) if rows else []
    with tmp.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
    tmp.replace(path)


class EnhancedStrategyEngine:
    def __init__(
        self,
        client: OkxRestClient,
        logger: Logger,
        *,
        notifier: EmailNotifier | None = None,
        strategy_name: str = PARENT_STRATEGY_NAME,
        session_id: str = "",
    ) -> None:
        self._client = client
        self._logger = logger
        self._notifier = notifier
        self._strategy_name = strategy_name or PARENT_STRATEGY_NAME
        self._session_id = session_id
        self._api_name = ""
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._order_ref_counter = 0

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    def start(self, credentials: Credentials, config: StrategyConfig) -> None:
        self.start_with_context(credentials, config)

    def start_with_context(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        started_at: datetime | None = None,
        recovery_root_dir: Path | None = None,
    ) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("策略已经在运行中")
            self._stop_event.clear()
            self._api_name = credentials.profile_name.strip()
            self._thread = threading.Thread(
                target=self._run,
                args=(credentials, config, started_at, recovery_root_dir),
                daemon=True,
                name=f"okx-{config.strategy_id}-enhanced",
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def wait_stopped(self, timeout: float | None = None) -> bool:
        with self._lock:
            thread = self._thread
        if thread is None:
            return True
        thread.join(timeout=timeout)
        return not thread.is_alive()

    def _run(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        started_at: datetime | None = None,
        recovery_root_dir: Path | None = None,
    ) -> None:
        state: _EngineState | None = None
        try:
            state = self._bootstrap_state(
                credentials,
                config,
                started_at=started_at,
                recovery_root_dir=recovery_root_dir,
            )
            self._logger(
                "现货增强三十六计已启动 | "
                f"信号={state.signal_inst_id} | 交易={state.trade_inst_id} | "
                f"单槽数量={format_decimal(state.slot_size)} | "
                f"每方向最大槽位={state.max_slots_per_direction}"
            )
            self._logger(f"运行目录：{state.artifacts.root_dir}")
            self._logger(f"人工池看板：{state.artifacts.manual_html}")
            self._write_state_snapshot(state, force=True)
            while not self._stop_event.is_set():
                try:
                    self._run_cycle(credentials, config, state)
                except Exception as exc:
                    self._log_error(config, f"增强策略轮询异常：{exc}")
                self._stop_event.wait(max(config.poll_seconds, 1.0))
        except Exception as exc:
            self._log_error(config, str(exc))
            self._logger(f"增强策略停止，原因：{exc}")
        finally:
            if state is not None:
                self._write_state_snapshot(state, force=True)
            self._stop_event.set()

    def _bootstrap_state(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        started_at: datetime | None = None,
        recovery_root_dir: Path | None = None,
    ) -> _EngineState:
        if config.run_mode != "trade":
            raise RuntimeError("现货增强三十六计当前只支持自动交易模式")
        trade_inst_id = derive_swap_trade_inst_id(config.trade_inst_id or config.inst_id)
        signal_inst_id = derive_spot_signal_inst_id(config.inst_id or trade_inst_id)
        if infer_inst_type(trade_inst_id) != "SWAP":
            raise RuntimeError("现货增强三十六计当前第一版只支持永续合约执行")
        trade_instrument = self._client.get_instrument(trade_inst_id)
        if trade_instrument.state.lower() != "live":
            raise RuntimeError(f"{trade_inst_id} 当前不可交易，状态：{trade_instrument.state}")
        signal_instrument = self._client.get_instrument(signal_inst_id)
        if signal_instrument.state.lower() != "live":
            raise RuntimeError(f"{signal_inst_id} 当前不可用作信号源，状态：{signal_instrument.state}")
        slot_size = snap_to_increment(config.order_size, trade_instrument.lot_size, "down")
        if slot_size < trade_instrument.min_size:
            raise RuntimeError(
                f"固定数量 {format_decimal(slot_size)} 小于最小下单量 {format_decimal(trade_instrument.min_size)}"
            )
        if config.max_entries_per_trend <= 0:
            raise RuntimeError("每波最多开仓次数必须大于 0，这里会当作每个方向的最大槽位数")

        registry = EnhancedStrategyRegistry()
        register_seed_strategy_package(
            registry,
            spot_inst_id=signal_inst_id,
            signal_bar=config.bar,
        )
        runtime_config_path = enhanced_strategy_runtime_file_path()
        store = load_runtime_store(runtime_config_path)
        payload = get_strategy_runtime_payload(store, PARENT_STRATEGY_ID)
        if payload is not None:
            apply_strategy_runtime_payload(
                registry,
                parent_strategy_id=PARENT_STRATEGY_ID,
                strategy_payload=payload,
            )
        enabled_signals = _filter_enabled_signals(registry, config.signal_mode)
        if not enabled_signals:
            raise RuntimeError("当前没有启用的子策略可执行")

        started_at = started_at or datetime.now()
        artifacts = build_live_report_artifact_paths(
            started_at=started_at,
            session_id=self._session_id or "session",
            trade_inst_id=trade_inst_id,
            root_dir=recovery_root_dir,
        )
        total_qty = slot_size * Decimal(str(config.max_entries_per_trend))
        long_total = total_qty if config.signal_mode in {"both", "long_only"} else ZERO
        short_total = total_qty if config.signal_mode in {"both", "short_only"} else ZERO
        state = _EngineState(
            started_at=started_at,
            artifacts=artifacts,
            registry=registry,
            trade_inst_id=trade_inst_id,
            signal_inst_id=signal_inst_id,
            slot_size=slot_size,
            max_slots_per_direction=config.max_entries_per_trend,
            long_quota=DirectionQuota(total=long_total),
            short_quota=DirectionQuota(total=short_total),
            enabled_signal_ids=tuple(item.signal_id for item in enabled_signals),
        )
        if recovery_root_dir is not None:
            self._restore_state_snapshot(state)
        else:
            _write_json(
                artifacts.manifest_json,
                {
                    "schema_version": 1,
                    "created_at": _iso_now(),
                    "strategy_id": config.strategy_id,
                    "strategy_name": self._strategy_name,
                    "session_id": self._session_id,
                    "signal_inst_id": signal_inst_id,
                    "trade_inst_id": trade_inst_id,
                    "environment": config.environment,
                    "signal_mode": config.signal_mode,
                    "slot_size": format(slot_size, "f"),
                    "max_slots_per_direction": config.max_entries_per_trend,
                    "runtime_config_path": str(runtime_config_path),
                },
            )
            state.seen_fill_keys = {
                _fill_key(item)
                for item in self._safe_get_fills_history(credentials, config)
            }
        self._bootstrap_existing_positions(credentials, config, state)
        return state

    def _bootstrap_existing_positions(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        state: _EngineState,
    ) -> None:
        tracked_by_direction = {
            "long": sum((item.quantity for item in state.active_positions.values() if item.direction == "long"), ZERO),
            "short": sum((item.quantity for item in state.active_positions.values() if item.direction == "short"), ZERO),
        }
        positions = self._client.get_positions(credentials, environment=config.environment, inst_type="SWAP")
        for raw in positions:
            if raw.inst_id != state.trade_inst_id:
                continue
            direction = _position_direction(raw)
            if direction is None:
                continue
            if config.signal_mode == "long_only" and direction != "long":
                continue
            if config.signal_mode == "short_only" and direction != "short":
                continue
            actual_quantity = abs(raw.position)
            if actual_quantity <= 0:
                continue
            tracked_quantity = tracked_by_direction[direction]
            surplus_quantity = actual_quantity - tracked_quantity
            if surplus_quantity <= ZERO:
                continue
            _direction_quota(state, direction).used += surplus_quantity
            tracked = LiveEnhancedPosition(
                position_id=f"baseline-{direction}-{len(state.active_positions) + 1:02d}",
                signal_id=f"baseline_{direction}",
                signal_name="恢复基线仓" if tracked_quantity > 0 else "启动前已有仓位",
                playbook_id=f"baseline_{direction}",
                playbook_name="人工接管基线仓",
                playbook_action="SWAP_LONG" if direction == "long" else "SWAP_SHORT",
                direction=direction,
                source_inst_id=state.trade_inst_id,
                source_bar=config.bar,
                trade_inst_id=state.trade_inst_id,
                quantity=surplus_quantity,
                signal_price=raw.avg_price or raw.mark_price or ZERO,
                entry_price=raw.avg_price or raw.mark_price or ZERO,
                entry_ts=_ts_from_position(raw),
                stop_loss_price=None,
                take_profit_price=None,
                max_hold_bars=0,
                fee_rate=ZERO,
                slippage_rate=ZERO,
                trigger_reason="恢复时发现账户存在未追踪仓位，自动纳入人工池" if tracked_quantity > 0 else "启动前已有仓位，自动纳入人工池",
                pos_side=direction if config.position_mode == "long_short" else None,
                last_reference_price=raw.last_price or raw.mark_price or raw.avg_price,
                last_reference_ts=_ts_from_position(raw),
                status="manual_managed",
                handoff_ts=_ts_from_position(raw),
                handoff_reason="恢复时发现未追踪仓位" if tracked_quantity > 0 else "启动前已有仓位",
            )
            state.active_positions[tracked.position_id] = tracked
            tracked_by_direction[direction] += surplus_quantity
            self._append_event(
                state,
                {
                    "event": "baseline_imported",
                    "position_id": tracked.position_id,
                    "direction": direction,
                    "quantity": format(tracked.quantity, "f"),
                    "entry_price": format(tracked.entry_price, "f"),
                    "ts": _iso_now(),
                },
            )

    def _restore_state_snapshot(self, state: _EngineState) -> None:
        if not state.artifacts.manifest_json.exists():
            raise RuntimeError(f"恢复失败：缺少会话清单 {state.artifacts.manifest_json}")
        if not state.artifacts.state_json.exists():
            raise RuntimeError(f"恢复失败：缺少状态快照 {state.artifacts.state_json}")

        manifest_payload = json.loads(state.artifacts.manifest_json.read_text(encoding="utf-8"))
        state_payload = json.loads(state.artifacts.state_json.read_text(encoding="utf-8"))
        manifest_trade_inst_id = str(manifest_payload.get("trade_inst_id", "")).strip().upper()
        if manifest_trade_inst_id and manifest_trade_inst_id != state.trade_inst_id:
            raise RuntimeError(
                f"恢复失败：快照交易标的 {manifest_trade_inst_id} 与当前配置 {state.trade_inst_id} 不一致"
            )

        restored_positions: dict[str, LiveEnhancedPosition] = {}
        raw_positions = state_payload.get("active_positions", [])
        if isinstance(raw_positions, list):
            for raw_item in raw_positions:
                position = self._deserialize_position_snapshot(raw_item)
                if position is None:
                    continue
                restored_positions[position.position_id] = position
        state.active_positions = restored_positions

        quota_payload = state_payload.get("quota")
        tracked_long = sum((item.quantity for item in restored_positions.values() if item.direction == "long"), ZERO)
        tracked_short = sum((item.quantity for item in restored_positions.values() if item.direction == "short"), ZERO)
        if isinstance(quota_payload, dict):
            state.long_quota.total = _deserialize_decimal(quota_payload.get("long_total"), state.long_quota.total)
            state.short_quota.total = _deserialize_decimal(quota_payload.get("short_total"), state.short_quota.total)
            state.long_quota.used = max(_deserialize_decimal(quota_payload.get("long_used"), tracked_long), tracked_long)
            state.short_quota.used = max(_deserialize_decimal(quota_payload.get("short_used"), tracked_short), tracked_short)
        else:
            state.long_quota.used = tracked_long
            state.short_quota.used = tracked_short

        counts_payload = state_payload.get("counts")
        if isinstance(counts_payload, dict):
            state.closed_position_count = max(int(counts_payload.get("closed_positions", 0)), 0)
            state.event_count = max(int(counts_payload.get("events", 0)), 0)

        raw_events = state_payload.get("recent_events", [])
        if isinstance(raw_events, list):
            state.recent_events = [item for item in raw_events if isinstance(item, dict)]
        state.event_count = max(state.event_count, len(state.recent_events))
        state.processed_signal_keys = {
            str(item).strip()
            for item in state_payload.get("processed_signal_keys", [])
            if str(item).strip()
        }
        state.seen_fill_keys = {
            str(item).strip()
            for item in state_payload.get("seen_fill_keys", [])
            if str(item).strip()
        }
        state.engine_order_ids = {
            str(item).strip()
            for item in state_payload.get("engine_order_ids", [])
            if str(item).strip()
        }
        state.engine_client_order_ids = {
            str(item).strip()
            for item in state_payload.get("engine_client_order_ids", [])
            if str(item).strip()
        }
        restored_manual = sum(1 for item in restored_positions.values() if item.status == "manual_managed")
        restored_auto = len(restored_positions) - restored_manual
        self._logger(
            "已加载恢复快照 | "
            f"目录={state.artifacts.root_dir} | 自动仓位={restored_auto} | 人工池={restored_manual}"
        )

    @staticmethod
    def _deserialize_position_snapshot(payload: object) -> LiveEnhancedPosition | None:
        if not isinstance(payload, dict):
            return None
        position_id = str(payload.get("position_id", "")).strip()
        direction = str(payload.get("direction", "")).strip().lower()
        trade_inst_id = str(payload.get("trade_inst_id", "")).strip().upper()
        source_inst_id = str(payload.get("source_inst_id", trade_inst_id)).strip().upper()
        source_bar = str(payload.get("source_bar", "")).strip() or "5m"
        if not position_id or direction not in {"long", "short"} or not trade_inst_id:
            return None
        pos_side_raw = str(payload.get("pos_side", "")).strip().lower()
        pos_side: Literal["long", "short"] | None = pos_side_raw if pos_side_raw in {"long", "short"} else None
        status_raw = str(payload.get("status", "")).strip().lower()
        manual_pool = str(payload.get("manual_pool", "")).strip().lower()
        status: Literal["opened_auto", "manual_managed"]
        status = "manual_managed" if status_raw == "manual_managed" or manual_pool == "manual" else "opened_auto"
        try:
            entry_ts = int(payload.get("entry_ts", _now_ms()))
        except Exception:
            entry_ts = _now_ms()
        max_hold_bars = 0
        try:
            max_hold_bars = max(int(payload.get("max_hold_bars", 0)), 0)
        except Exception:
            max_hold_bars = 0
        return LiveEnhancedPosition(
            position_id=position_id,
            signal_id=str(payload.get("signal_id", "")).strip(),
            signal_name=str(payload.get("signal_name", "")).strip(),
            playbook_id=str(payload.get("playbook_id", "")).strip(),
            playbook_name=str(payload.get("playbook_name", "")).strip(),
            playbook_action=str(
                payload.get("playbook_action", "SWAP_LONG" if direction == "long" else "SWAP_SHORT")
            ).strip(),
            direction=direction,
            source_inst_id=source_inst_id,
            source_bar=source_bar,
            trade_inst_id=trade_inst_id,
            quantity=_deserialize_decimal(payload.get("quantity")),
            signal_price=_deserialize_decimal(payload.get("signal_price")),
            entry_price=_deserialize_decimal(payload.get("entry_price")),
            entry_ts=entry_ts,
            stop_loss_price=_deserialize_optional_decimal(payload.get("stop_loss_price")),
            take_profit_price=_deserialize_optional_decimal(payload.get("take_profit_price")),
            max_hold_bars=max_hold_bars,
            fee_rate=_deserialize_decimal(payload.get("fee_rate")),
            slippage_rate=_deserialize_decimal(payload.get("slippage_rate")),
            trigger_reason=str(payload.get("trigger_reason", "")).strip(),
            pos_side=pos_side,
            entry_order_id=str(payload.get("entry_order_id", "")).strip() or None,
            entry_client_order_id=str(payload.get("entry_client_order_id", "")).strip() or None,
            last_reference_price=_deserialize_optional_decimal(
                payload.get("last_reference_price", payload.get("current_price"))
            ),
            last_reference_ts=_deserialize_optional_int(payload.get("last_reference_ts")),
            last_evaluated_candle_ts=_deserialize_optional_int(payload.get("last_evaluated_candle_ts")),
            status=status,
            handoff_ts=_deserialize_optional_int(payload.get("handoff_ts")),
            handoff_reason=str(payload.get("handoff_reason", "")).strip(),
        )

    def _run_cycle(self, credentials: Credentials, config: StrategyConfig, state: _EngineState) -> None:
        fills = self._safe_get_fills_history(credentials, config)
        self._process_external_fills(state, fills)
        candles_by_source = self._load_source_candles(state)
        latest_prices = self._load_latest_prices(state)
        self._refresh_reference_marks(state, candles_by_source, latest_prices)
        self._manage_auto_positions(credentials, config, state, candles_by_source, latest_prices)
        self._evaluate_new_signals(credentials, config, state, candles_by_source)
        self._reconcile_account_positions(credentials, config, state)
        self._write_state_snapshot(state)

    def _load_source_candles(self, state: _EngineState) -> dict[tuple[str, str], list[Candle]]:
        data: dict[tuple[str, str], list[Candle]] = {}
        sources = {
            (signal.source.inst_id, signal.source.bar)
            for signal in state.registry.list_child_signals(PARENT_STRATEGY_ID, enabled_only=False)
            if signal.signal_id in state.enabled_signal_ids
        }
        for inst_id, bar in sources:
            candles = self._client.get_candles(inst_id, bar, limit=DEFAULT_SIGNAL_LOOKBACK)
            confirmed = [item for item in candles if item.confirmed]
            if confirmed:
                data[(inst_id, bar)] = confirmed
        return data

    def _load_latest_prices(self, state: _EngineState) -> dict[str, Decimal]:
        prices: dict[str, Decimal] = {}
        targets = {state.trade_inst_id}
        for signal in state.registry.list_child_signals(PARENT_STRATEGY_ID, enabled_only=False):
            if signal.signal_id in state.enabled_signal_ids:
                targets.add(signal.source.inst_id)
        for inst_id in targets:
            try:
                ticker = self._client.get_ticker(inst_id)
            except Exception:
                continue
            price = ticker.last or ticker.mark or ticker.index or ticker.bid or ticker.ask
            if price is not None and price > 0:
                prices[inst_id] = price
        return prices

    def _refresh_reference_marks(
        self,
        state: _EngineState,
        candles_by_source: dict[tuple[str, str], list[Candle]],
        latest_prices: dict[str, Decimal],
    ) -> None:
        for position in state.active_positions.values():
            confirmed = candles_by_source.get((position.source_inst_id, position.source_bar), [])
            if confirmed:
                position.last_reference_ts = confirmed[-1].ts
                position.last_reference_price = latest_prices.get(position.source_inst_id, confirmed[-1].close)
            elif position.trade_inst_id in latest_prices:
                position.last_reference_ts = position.last_reference_ts or _now_ms()
                position.last_reference_price = latest_prices[position.trade_inst_id]

    def _manage_auto_positions(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        state: _EngineState,
        candles_by_source: dict[tuple[str, str], list[Candle]],
        latest_prices: dict[str, Decimal],
    ) -> None:
        for position_id in list(state.active_positions.keys()):
            position = state.active_positions.get(position_id)
            if position is None or position.status != "opened_auto":
                continue
            confirmed = candles_by_source.get((position.source_inst_id, position.source_bar), [])
            if not confirmed:
                continue
            current_price = latest_prices.get(position.source_inst_id, confirmed[-1].close)
            latest_candle = confirmed[-1]
            if position.take_profit_price is not None and _target_hit(
                direction=position.direction,
                current_price=current_price,
                target_price=position.take_profit_price,
                kind="tp",
            ):
                self._close_position(credentials, config, state, position, reason="达到止盈目标")
                continue
            if position.stop_loss_price is not None and _target_hit(
                direction=position.direction,
                current_price=current_price,
                target_price=position.stop_loss_price,
                kind="sl",
            ):
                self._handoff_position(state, position, reason="触发人工接管线", handoff_ts=latest_candle.ts)
                continue
            if position.last_evaluated_candle_ts == latest_candle.ts:
                continue
            position.last_evaluated_candle_ts = latest_candle.ts
            if position.max_hold_bars > 0 and _holding_bars(position.entry_ts, latest_candle.ts, position.source_bar) >= position.max_hold_bars:
                if _directional_pnl(position, current_price) >= ZERO:
                    self._close_position(credentials, config, state, position, reason="超过最大持有K线数后盈利退出")
                else:
                    self._handoff_position(state, position, reason="超过最大持有K线数后转人工", handoff_ts=latest_candle.ts)

    def _evaluate_new_signals(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        state: _EngineState,
        candles_by_source: dict[tuple[str, str], list[Candle]],
    ) -> None:
        for signal in state.registry.list_child_signals(PARENT_STRATEGY_ID, enabled_only=False):
            if signal.signal_id not in state.enabled_signal_ids:
                continue
            confirmed = candles_by_source.get((signal.source.inst_id, signal.source.bar), [])
            if not confirmed:
                continue
            index = len(confirmed) - 1
            trigger_rule = state.registry.get_trigger_rule(signal.trigger_rule_id)
            match = trigger_rule(confirmed, index)
            if match is None or not match.triggered:
                continue
            event_key = f"{signal.signal_id}:{confirmed[-1].ts}"
            if event_key in state.processed_signal_keys:
                continue
            signal_price = match.signal_price or confirmed[-1].close
            opened = False
            for playbook in state.registry.list_playbooks_for_signal(signal.signal_id, enabled_only=True):
                if playbook.action not in SUPPORTED_PLAYBOOK_ACTIONS:
                    continue
                direction: Literal["long", "short"] = "long" if playbook.action == "SWAP_LONG" else "short"
                if not _reserve_quota(state, direction, state.slot_size):
                    self._logger(
                        f"信号已触发但额度已满 | {signal.signal_name} | "
                        f"{direction} 可用={format_decimal(_direction_quota(state, direction).available)}"
                    )
                    continue
                try:
                    self._open_position(
                        credentials=credentials,
                        config=config,
                        state=state,
                        signal=signal,
                        direction=direction,
                        playbook_name=playbook.playbook_name,
                        playbook_id=playbook.playbook_id,
                        playbook_action=playbook.action,
                        reason=match.reason,
                        signal_price=signal_price,
                        candle_ts=confirmed[-1].ts,
                    )
                    opened = True
                except Exception:
                    _release_quota(state, direction, state.slot_size)
                    raise
            if opened:
                state.processed_signal_keys.add(event_key)

    def _open_position(
        self,
        *,
        credentials: Credentials,
        config: StrategyConfig,
        state: _EngineState,
        signal: ChildSignalConfig,
        direction: Literal["long", "short"],
        playbook_name: str,
        playbook_id: str,
        playbook_action: str,
        reason: str,
        signal_price: Decimal,
        candle_ts: int,
    ) -> None:
        side = "buy" if direction == "long" else "sell"
        pos_side = direction if config.position_mode == "long_short" else None
        result = self._client.place_simple_order(
            credentials,
            config,
            inst_id=state.trade_inst_id,
            side=side,
            size=state.slot_size,
            ord_type="market",
            pos_side=pos_side,
            cl_ord_id=self._next_client_order_id(role="ent"),
        )
        fill_price, fill_qty = self._resolve_fill_price(
            credentials,
            config,
            inst_id=state.trade_inst_id,
            ord_id=result.ord_id,
            fallback_price=signal_price,
            fallback_size=state.slot_size,
        )
        if fill_qty != state.slot_size:
            quota = _direction_quota(state, direction)
            quota.used = max(quota.used - state.slot_size + fill_qty, ZERO)
        state.engine_order_ids.add(result.ord_id)
        if result.cl_ord_id:
            state.engine_client_order_ids.add(result.cl_ord_id)
        profile = _signal_profile(state.registry, signal.signal_id)
        position = LiveEnhancedPosition(
            position_id=f"{signal.signal_id}-{candle_ts}-{len(state.active_positions) + 1:03d}",
            signal_id=signal.signal_id,
            signal_name=signal.signal_name,
            playbook_id=playbook_id,
            playbook_name=playbook_name,
            playbook_action=playbook_action,
            direction=direction,
            source_inst_id=signal.source.inst_id,
            source_bar=signal.source.bar,
            trade_inst_id=state.trade_inst_id,
            quantity=fill_qty,
            signal_price=signal_price,
            entry_price=fill_price,
            entry_ts=candle_ts,
            stop_loss_price=_stop_price(direction, fill_price, Decimal(str(profile["stop_loss_pct"]))),
            take_profit_price=_take_price(direction, fill_price, Decimal(str(profile["take_profit_pct"]))),
            max_hold_bars=int(profile["max_hold_bars"]),
            fee_rate=Decimal(str(profile["fee_rate"])),
            slippage_rate=Decimal(str(profile["slippage_rate"])),
            trigger_reason=reason,
            pos_side=pos_side,
            entry_order_id=result.ord_id,
            entry_client_order_id=result.cl_ord_id,
            last_reference_price=signal_price,
            last_reference_ts=candle_ts,
        )
        state.active_positions[position.position_id] = position
        self._append_event(
            state,
            {
                "event": "position_opened",
                "position_id": position.position_id,
                "signal_id": signal.signal_id,
                "signal_name": signal.signal_name,
                "direction": direction,
                "quantity": format(fill_qty, "f"),
                "signal_price": format(signal_price, "f"),
                "entry_price": format(fill_price, "f"),
                "reason": reason,
                "ts": _iso_now(),
            },
        )
        self._logger(
            "增强策略开仓 | "
            f"{signal.signal_name} | 方向={direction} | 数量={format_decimal(fill_qty)} | 成交价={format_decimal(fill_price)}"
        )
        if self._notifier is not None:
            self._notifier.send_trade_fill(
                strategy_name=self._strategy_name,
                config=config,
                title=f"{signal.signal_name} 开仓",
                symbol=state.trade_inst_id,
                side=side,
                size=format_decimal(fill_qty),
                price=format_decimal(fill_price),
                reason=reason,
                api_name=self._api_name,
            )

    def _close_position(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        state: _EngineState,
        position: LiveEnhancedPosition,
        *,
        reason: str,
    ) -> None:
        result = self._client.place_simple_order(
            credentials,
            config,
            inst_id=position.trade_inst_id,
            side=position.close_side,
            size=position.quantity,
            ord_type="market",
            pos_side=position.pos_side,
            cl_ord_id=self._next_client_order_id(role="exi"),
        )
        fill_price, fill_qty = self._resolve_fill_price(
            credentials,
            config,
            inst_id=position.trade_inst_id,
            ord_id=result.ord_id,
            fallback_price=position.last_reference_price or position.entry_price,
            fallback_size=position.quantity,
        )
        state.engine_order_ids.add(result.ord_id)
        if result.cl_ord_id:
            state.engine_client_order_ids.add(result.cl_ord_id)
        _release_quota(state, position.direction, position.quantity)
        state.closed_position_count += 1
        state.active_positions.pop(position.position_id, None)
        realized_pnl = _directional_points(
            entry_price=position.entry_price,
            exit_price=fill_price,
            direction=position.direction,
        ) * fill_qty
        self._append_event(
            state,
            {
                "event": "position_closed_auto",
                "position_id": position.position_id,
                "direction": position.direction,
                "quantity": format(fill_qty, "f"),
                "exit_price": format(fill_price, "f"),
                "realized_pnl": format(realized_pnl, "f"),
                "reason": reason,
                "ts": _iso_now(),
            },
        )
        self._logger(
            "增强策略自动平仓 | "
            f"{position.signal_name} | 数量={format_decimal(fill_qty)} | 价格={format_decimal(fill_price)} | "
            f"预估盈亏={format_decimal(realized_pnl)}"
        )

    def _handoff_position(
        self,
        state: _EngineState,
        position: LiveEnhancedPosition,
        *,
        reason: str,
        handoff_ts: int,
    ) -> None:
        state.active_positions[position.position_id] = replace(
            position,
            status="manual_managed",
            handoff_reason=reason,
            handoff_ts=handoff_ts,
        )
        self._append_event(
            state,
            {
                "event": "position_handoff_manual",
                "position_id": position.position_id,
                "direction": position.direction,
                "quantity": format(position.quantity, "f"),
                "reason": reason,
                "ts": _iso_now(),
            },
        )
        self._logger(
            f"错单转人工 | {position.signal_name} | 方向={position.direction} | 数量={format_decimal(position.quantity)} | {reason}"
        )

    def _process_external_fills(self, state: _EngineState, fills: list[OkxFillHistoryItem]) -> None:
        for fill in fills:
            key = _fill_key(fill)
            if key in state.seen_fill_keys:
                continue
            state.seen_fill_keys.add(key)
            if fill.inst_id != state.trade_inst_id:
                continue
            if fill.order_id and fill.order_id in state.engine_order_ids:
                continue
            if fill.raw.get("clOrdId") and str(fill.raw.get("clOrdId")) in state.engine_client_order_ids:
                continue
            if fill.fill_size is None or fill.fill_size <= 0:
                continue
            direction = _manual_fill_close_direction(fill)
            if direction is None:
                continue
            new_positions, reduced = apply_external_fill_reduction(
                list(state.active_positions.values()),
                quantity=fill.fill_size,
                direction=direction,
            )
            if not reduced:
                continue
            state.active_positions = {item.position_id: item for item in new_positions}
            released_qty = ZERO
            for item in reduced:
                released_qty += item.closed_qty
                _release_quota(state, direction, item.closed_qty)
            self._append_event(
                state,
                {
                    "event": "position_reduced_manual_fill",
                    "direction": direction,
                    "closed_qty": format(released_qty, "f"),
                    "fill_price": "" if fill.fill_price is None else format(fill.fill_price, "f"),
                    "ts": _iso_now(),
                },
            )

    def _reconcile_account_positions(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        state: _EngineState,
    ) -> None:
        actual = {"long": ZERO, "short": ZERO}
        for raw in self._client.get_positions(credentials, environment=config.environment, inst_type="SWAP"):
            if raw.inst_id != state.trade_inst_id:
                continue
            direction = _position_direction(raw)
            if direction is not None:
                actual[direction] += raw.position
        tracked = {
            "long": sum((item.quantity for item in state.active_positions.values() if item.direction == "long"), ZERO),
            "short": sum((item.quantity for item in state.active_positions.values() if item.direction == "short"), ZERO),
        }
        for direction in ("long", "short"):
            deficit = tracked[direction] - actual[direction]
            if deficit <= ZERO:
                continue
            new_positions, reduced = apply_external_fill_reduction(
                list(state.active_positions.values()),
                quantity=deficit,
                direction=direction,
            )
            if not reduced:
                continue
            state.active_positions = {item.position_id: item for item in new_positions}
            released_qty = sum((item.closed_qty for item in reduced), ZERO)
            _release_quota(state, direction, released_qty)
            self._append_event(
                state,
                {
                    "event": "position_reduced_reconcile",
                    "direction": direction,
                    "closed_qty": format(released_qty, "f"),
                    "ts": _iso_now(),
                },
            )

    def _resolve_fill_price(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        ord_id: str,
        fallback_price: Decimal,
        fallback_size: Decimal,
    ) -> tuple[Decimal, Decimal]:
        price = fallback_price
        size = fallback_size
        for _ in range(3):
            try:
                status = self._client.get_order(credentials, config, inst_id=inst_id, ord_id=ord_id)
            except Exception:
                continue
            if status.avg_price is not None and status.avg_price > 0:
                price = status.avg_price
            if status.filled_size is not None and status.filled_size > 0:
                size = status.filled_size
            if status.state.lower() in {"filled", "partially_filled"}:
                break
        return price, size

    def _write_state_snapshot(self, state: _EngineState, *, force: bool = False) -> None:
        now = datetime.now(timezone.utc)
        if not force and state.last_state_flush_at is not None:
            if (now - state.last_state_flush_at).total_seconds() < DEFAULT_STATE_FLUSH_SECONDS and not state.events_dirty:
                return
        all_rows = [self._position_snapshot_row(item) for item in state.active_positions.values()]
        manual_rows = [row for row in all_rows if row["manual_pool"] == "manual"]
        _write_json(
            state.artifacts.state_json,
            {
                "schema_version": 1,
                "updated_at": _iso_now(),
                "trade_inst_id": state.trade_inst_id,
                "signal_inst_id": state.signal_inst_id,
                "quota": {
                    "long_total": format(state.long_quota.total, "f"),
                    "long_used": format(state.long_quota.used, "f"),
                    "short_total": format(state.short_quota.total, "f"),
                    "short_used": format(state.short_quota.used, "f"),
                },
                "counts": {
                    "active_positions": len(all_rows),
                    "manual_positions": len(manual_rows),
                    "auto_positions": len(all_rows) - len(manual_rows),
                    "closed_positions": state.closed_position_count,
                    "events": state.event_count,
                },
                "active_positions": [_serialize_value(item) for item in all_rows],
                "recent_events": [_serialize_value(item) for item in state.recent_events[-200:]],
                "processed_signal_keys": sorted(state.processed_signal_keys),
                "seen_fill_keys": sorted(state.seen_fill_keys),
                "engine_order_ids": sorted(state.engine_order_ids),
                "engine_client_order_ids": sorted(state.engine_client_order_ids),
            },
        )
        _write_csv(state.artifacts.active_csv, [_stringify_row(item) for item in all_rows])
        _write_csv(state.artifacts.manual_csv, [_stringify_row(item) for item in manual_rows])
        self._write_manual_dashboard(state, manual_rows)
        state.last_state_flush_at = now
        state.events_dirty = False

    def _write_manual_dashboard(self, state: _EngineState, manual_rows: list[dict[str, object]]) -> None:
        cards = []
        for row in manual_rows:
            pnl = Decimal(str(row["estimated_pnl"]))
            pnl_class = "profit" if pnl >= ZERO else "loss"
            cards.append(
                "<tr>"
                f"<td>{escape(str(row['signal_name']))}</td>"
                f"<td>{escape(str(row['direction']))}</td>"
                f"<td>{escape(str(row['quantity']))}</td>"
                f"<td>{escape(str(row['entry_price']))}</td>"
                f"<td>{escape(str(row['current_price']))}</td>"
                f"<td>{escape(str(row['break_even_price']))}</td>"
                f"<td>{escape(str(row['take_profit_price']))}</td>"
                f"<td>{escape(str(row['stop_loss_price']))}</td>"
                f"<td class=\"{pnl_class}\">{escape(str(row['estimated_pnl']))}</td>"
                f"<td>{escape(str(row['handoff_reason']))}</td>"
                f"<td>{escape(str(row['entry_time']))}</td>"
                "</tr>"
            )
        html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{escape(PARENT_STRATEGY_NAME)} 人工池看板</title>
  <style>
    body {{
      margin: 0;
      color: #1a2330;
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      background: linear-gradient(180deg, #faf7f0 0%, #f1ece2 100%);
    }}
    header {{ padding: 24px 28px 16px; border-bottom: 1px solid rgba(0,0,0,0.08); }}
    h1 {{ margin: 0 0 8px; font-size: 28px; }}
    p {{ margin: 0; color: #596372; }}
    main {{ padding: 20px 24px 30px; }}
    .panel {{
      background: rgba(255,255,255,0.96);
      border: 1px solid #ded6c8;
      border-radius: 16px;
      overflow: auto;
      box-shadow: 0 18px 28px rgba(26, 36, 46, 0.06);
    }}
    table {{ width: 100%; border-collapse: collapse; min-width: 1050px; }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid rgba(0,0,0,0.06); text-align: left; white-space: nowrap; font-size: 13px; }}
    th {{ background: #faf6ef; position: sticky; top: 0; }}
    .profit {{ color: #1b6b54; font-weight: 700; }}
    .loss {{ color: #bb4a42; font-weight: 700; }}
  </style>
</head>
<body>
  <header>
    <h1>{escape(PARENT_STRATEGY_NAME)} 人工池看板</h1>
    <p>交易标的：{escape(state.trade_inst_id)}。这里集中展示已转人工的仓位，便于你直接按进场理由和净保本价统一处理。</p>
  </header>
  <main>
    <div class="panel">
      <table>
        <thead>
          <tr>
            <th>子策略</th>
            <th>方向</th>
            <th>数量</th>
            <th>进场价</th>
            <th>当前价</th>
            <th>净保本价</th>
            <th>止盈价</th>
            <th>人工线</th>
            <th>当前盈亏</th>
            <th>转人工原因</th>
            <th>进场时间</th>
          </tr>
        </thead>
        <tbody>
          {''.join(cards) or '<tr><td colspan="11">当前没有人工池仓位。</td></tr>'}
        </tbody>
      </table>
    </div>
  </main>
</body>
</html>
"""
        state.artifacts.manual_html.parent.mkdir(parents=True, exist_ok=True)
        state.artifacts.manual_html.write_text(html, encoding="utf-8")

    def _position_snapshot_row(self, position: LiveEnhancedPosition) -> dict[str, object]:
        current_price = position.last_reference_price or position.entry_price
        estimated_pnl = _directional_pnl(position, current_price)
        gross_break_even = position.entry_price
        return {
            "position_id": position.position_id,
            "signal_id": position.signal_id,
            "signal_name": position.signal_name,
            "playbook_id": position.playbook_id,
            "playbook_name": position.playbook_name,
            "playbook_action": position.playbook_action,
            "direction": position.direction,
            "status": position.status,
            "manual_pool": position.manual_pool,
            "quantity": position.quantity,
            "entry_price": position.entry_price,
            "signal_price": position.signal_price,
            "current_price": current_price,
            "break_even_price": gross_break_even,
            "take_profit_price": position.take_profit_price,
            "stop_loss_price": position.stop_loss_price,
            "estimated_pnl": estimated_pnl,
            "entry_ts": position.entry_ts,
            "entry_time": datetime.fromtimestamp(position.entry_ts / 1000, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S"),
            "max_hold_bars": position.max_hold_bars,
            "fee_rate": position.fee_rate,
            "slippage_rate": position.slippage_rate,
            "pos_side": position.pos_side,
            "entry_order_id": position.entry_order_id,
            "entry_client_order_id": position.entry_client_order_id,
            "last_reference_price": position.last_reference_price,
            "last_reference_ts": position.last_reference_ts,
            "last_evaluated_candle_ts": position.last_evaluated_candle_ts,
            "handoff_ts": position.handoff_ts,
            "handoff_reason": position.handoff_reason,
            "trigger_reason": position.trigger_reason,
            "source_inst_id": position.source_inst_id,
            "source_bar": position.source_bar,
            "trade_inst_id": position.trade_inst_id,
        }

    def _append_event(self, state: _EngineState, payload: dict[str, object]) -> None:
        item = {str(key): _serialize_value(value) for key, value in payload.items()}
        state.recent_events.append(item)
        state.event_count += 1
        state.events_dirty = True
        state.artifacts.events_jsonl.parent.mkdir(parents=True, exist_ok=True)
        with state.artifacts.events_jsonl.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(item, ensure_ascii=False))
            handle.write("\n")

    def _safe_get_fills_history(self, credentials: Credentials, config: StrategyConfig) -> list[OkxFillHistoryItem]:
        try:
            return self._client.get_fills_history(
                credentials,
                environment=config.environment,
                inst_types=("SWAP",),
                limit=DEFAULT_FILL_HISTORY_LIMIT,
            )
        except Exception:
            return []

    def _next_client_order_id(self, *, role: str) -> str:
        self._order_ref_counter += 1
        session_token = "".join(ch for ch in self._session_id.lower() if ch.isascii() and ch.isalnum())[:4] or "sess"
        role_token = "".join(ch for ch in role.lower() if ch.isascii() and ch.isalnum())[:3] or "ord"
        timestamp = datetime.utcnow().strftime("%m%d%H%M%S%f")[:-3]
        suffix = f"{self._order_ref_counter % 100:02d}"
        return f"{session_token}spot{role_token}{timestamp}{suffix}"[:32]

    def _log_error(self, config: StrategyConfig, message: str) -> None:
        if self._notifier is not None:
            try:
                self._notifier.send_error(
                    strategy_name=self._strategy_name,
                    config=config,
                    message=message,
                    api_name=self._api_name,
                )
            except Exception:
                pass
        self._logger(message)


def _stringify_row(row: dict[str, object]) -> dict[str, str]:
    return {str(key): "" if value is None else str(_serialize_value(value)) for key, value in row.items()}
