from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, fields as dataclass_fields
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from okx_quant.models import (
    DailyFilterBoundary,
    DailyFilterMode,
    DailyFilterScope,
    StrategyConfig,
)


STRATEGY_PROFILE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class DailyFilterSpec:
    enabled: bool = False
    symbol: str = ""
    bar: str = "1D"
    boundary: DailyFilterBoundary = "exchange"
    mode: DailyFilterMode = "disabled"
    scope: DailyFilterScope = "both"
    ma_type: str = "ema"
    period: int = 5
    notes: str = ""

    def to_payload(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_payload(cls, payload: object) -> "DailyFilterSpec":
        if not isinstance(payload, dict):
            return cls()
        return cls(
            enabled=bool(payload.get("enabled", False)),
            symbol=str(payload.get("symbol", "") or "").strip().upper(),
            bar=str(payload.get("bar", "1D") or "1D").strip(),
            boundary=str(payload.get("boundary", "exchange") or "exchange"),
            mode=str(payload.get("mode", "disabled") or "disabled"),
            scope=str(payload.get("scope", "both") or "both"),
            ma_type=str(payload.get("ma_type", "ema") or "ema").strip().lower(),
            period=max(int(payload.get("period", 5) or 5), 1),
            notes=str(payload.get("notes", "") or "").strip(),
        )


@dataclass(frozen=True)
class StrategyProfile:
    profile_id: str
    profile_name: str
    strategy_id: str
    symbol: str
    api_name: str = ""
    direction_label: str = ""
    run_mode_label: str = ""
    enabled: bool = True
    daily_filter: DailyFilterSpec = field(default_factory=DailyFilterSpec)
    config_snapshot: dict[str, object] = field(default_factory=dict)
    tags: tuple[str, ...] = ()
    notes: str = ""
    created_at: str = ""
    source_report: str = ""

    def to_payload(self) -> dict[str, object]:
        return {
            "profile_id": self.profile_id,
            "profile_name": self.profile_name,
            "strategy_id": self.strategy_id,
            "symbol": self.symbol,
            "api_name": self.api_name,
            "direction_label": self.direction_label,
            "run_mode_label": self.run_mode_label,
            "enabled": self.enabled,
            "daily_filter": self.daily_filter.to_payload(),
            "config_snapshot": dict(self.config_snapshot),
            "tags": list(self.tags),
            "notes": self.notes,
            "created_at": self.created_at,
            "source_report": self.source_report,
        }

    @classmethod
    def from_payload(cls, payload: object) -> "StrategyProfile":
        if not isinstance(payload, dict):
            raise ValueError("strategy profile payload must be an object")
        profile_id = str(payload.get("profile_id", "") or "").strip()
        strategy_id = str(payload.get("strategy_id", "") or "").strip()
        symbol = str(payload.get("symbol", "") or "").strip().upper()
        if not profile_id or not strategy_id or not symbol:
            raise ValueError("strategy profile missing profile_id, strategy_id, or symbol")
        config_snapshot = payload.get("config_snapshot")
        if not isinstance(config_snapshot, dict):
            raise ValueError("strategy profile missing config_snapshot")
        return cls(
            profile_id=profile_id,
            profile_name=str(payload.get("profile_name", profile_id) or profile_id).strip(),
            strategy_id=strategy_id,
            symbol=symbol,
            api_name=str(payload.get("api_name", "") or "").strip(),
            direction_label=str(payload.get("direction_label", "") or "").strip(),
            run_mode_label=str(payload.get("run_mode_label", "") or "").strip(),
            enabled=bool(payload.get("enabled", True)),
            daily_filter=DailyFilterSpec.from_payload(payload.get("daily_filter")),
            config_snapshot=dict(config_snapshot),
            tags=tuple(str(item).strip() for item in payload.get("tags", []) if str(item).strip()),
            notes=str(payload.get("notes", "") or "").strip(),
            created_at=str(payload.get("created_at", "") or "").strip(),
            source_report=str(payload.get("source_report", "") or "").strip(),
        )


@dataclass(frozen=True)
class StrategyBundle:
    bundle_version: int
    bundle_name: str
    profiles: tuple[StrategyProfile, ...]
    created_at: str = ""
    source_report: str = ""
    auto_start_on_import: bool = False

    def to_payload(self) -> dict[str, object]:
        return {
            "bundle_version": self.bundle_version,
            "bundle_name": self.bundle_name,
            "created_at": self.created_at,
            "source_report": self.source_report,
            "auto_start_on_import": self.auto_start_on_import,
            "profiles": [item.to_payload() for item in self.profiles],
        }

    @classmethod
    def from_payload(cls, payload: object) -> "StrategyBundle":
        if not isinstance(payload, dict):
            raise ValueError("strategy bundle payload must be an object")
        raw_profiles = payload.get("profiles")
        if not isinstance(raw_profiles, list) or not raw_profiles:
            raise ValueError("strategy bundle missing profiles")
        bundle_name = str(payload.get("bundle_name", "") or "").strip()
        if not bundle_name:
            raise ValueError("strategy bundle missing bundle_name")
        return cls(
            bundle_version=max(int(payload.get("bundle_version", STRATEGY_PROFILE_SCHEMA_VERSION) or STRATEGY_PROFILE_SCHEMA_VERSION), 1),
            bundle_name=bundle_name,
            profiles=tuple(StrategyProfile.from_payload(item) for item in raw_profiles),
            created_at=str(payload.get("created_at", "") or "").strip(),
            source_report=str(payload.get("source_report", "") or "").strip(),
            auto_start_on_import=bool(payload.get("auto_start_on_import", False)),
        )


def serialize_strategy_config_snapshot(config: StrategyConfig) -> dict[str, object]:
    snapshot: dict[str, object] = {}
    for item in dataclass_fields(StrategyConfig):
        value = getattr(config, item.name)
        if isinstance(value, Decimal):
            snapshot[item.name] = format(value, "f")
        elif isinstance(value, tuple):
            snapshot[item.name] = list(value)
        else:
            snapshot[item.name] = value
    return snapshot


def build_daily_filter_spec_from_config(config: StrategyConfig, *, notes: str = "") -> DailyFilterSpec:
    enabled = bool(config.daily_filter_enabled and config.daily_filter_mode != "disabled")
    return DailyFilterSpec(
        enabled=enabled,
        symbol=config.resolved_daily_filter_inst_id().upper(),
        bar=config.resolved_daily_filter_bar(),
        boundary=config.daily_filter_boundary,
        mode=config.daily_filter_mode,
        scope=config.daily_filter_scope,
        ma_type=str(config.daily_filter_ma_type or "ema").strip().lower(),
        period=max(int(config.daily_filter_period), 1),
        notes=notes.strip(),
    )


def build_strategy_profile_from_config(
    *,
    profile_id: str,
    profile_name: str,
    strategy_id: str,
    symbol: str,
    config: StrategyConfig,
    api_name: str = "",
    direction_label: str = "",
    run_mode_label: str = "",
    enabled: bool = True,
    tags: tuple[str, ...] = (),
    notes: str = "",
    source_report: str = "",
) -> StrategyProfile:
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    return StrategyProfile(
        profile_id=profile_id.strip(),
        profile_name=profile_name.strip(),
        strategy_id=strategy_id.strip(),
        symbol=symbol.strip().upper(),
        api_name=api_name.strip(),
        direction_label=direction_label.strip(),
        run_mode_label=run_mode_label.strip(),
        enabled=enabled,
        daily_filter=build_daily_filter_spec_from_config(config),
        config_snapshot=serialize_strategy_config_snapshot(config),
        tags=tuple(tag.strip() for tag in tags if tag.strip()),
        notes=notes.strip(),
        created_at=created_at,
        source_report=source_report.strip(),
    )


def write_strategy_bundle(bundle: StrategyBundle, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(bundle.to_payload(), ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)
    return path


def read_strategy_bundle(path: Path) -> StrategyBundle:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return StrategyBundle.from_payload(payload)
