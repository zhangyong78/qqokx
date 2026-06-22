from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

from okx_quant.app_paths import state_dir_path
from roll_terminal_qt.models import ArbitrageOpportunityView


CUSTOM_OPPORTUNITIES_FILE_NAME = "roll_terminal_custom_opportunities.json"


def default_opportunities() -> list[ArbitrageOpportunityView]:
    return [
        ArbitrageOpportunityView(
            key="btc_roll_near_next",
            title="BTC 交割换月",
            left_inst_id="BTC-USD-260626",
            right_inst_id="BTC-USD-260925",
            left_kind="交割",
            right_kind="交割",
            template="roll",
            description="当前交割 -> 远月交割，保留现货腿的专业移仓模板。",
        ),
        ArbitrageOpportunityView(
            key="btc_cash_futures",
            title="BTC 期现套利",
            left_inst_id="BTC-USD-260626",
            right_inst_id="BTC-USDT",
            left_kind="交割",
            right_kind="现货",
            template="professional",
            description="交割/现货双腿看盘与后续专业套利下单入口。",
        ),
        ArbitrageOpportunityView(
            key="btc_perp_spot",
            title="BTC 永续期现",
            left_inst_id="BTC-USDT-SWAP",
            right_inst_id="BTC-USDT",
            left_kind="永续",
            right_kind="现货",
            template="professional",
            description="永续/现货价差观察与双腿执行入口。",
        ),
        ArbitrageOpportunityView(
            key="eth_perp_spot",
            title="ETH 永续期现",
            left_inst_id="ETH-USDT-SWAP",
            right_inst_id="ETH-USDT",
            left_kind="永续",
            right_kind="现货",
            template="professional",
            description="ETH 永续与现货的专业套利模板。",
        ),
        ArbitrageOpportunityView(
            key="eth_roll_quarter",
            title="ETH 交割换月",
            left_inst_id="ETH-USD-260626",
            right_inst_id="ETH-USD-260925",
            left_kind="交割",
            right_kind="交割",
            template="roll",
            description="ETH 交割合约换月模板。",
        ),
    ]


def load_all_opportunities() -> list[ArbitrageOpportunityView]:
    return [*load_custom_opportunities(), *default_opportunities()]


def filter_opportunities(items: list[ArbitrageOpportunityView], keyword: str) -> list[ArbitrageOpportunityView]:
    query = keyword.strip().lower()
    if not query:
        return list(items)
    result: list[ArbitrageOpportunityView] = []
    for item in items:
        haystack = " | ".join(
            (
                item.title,
                item.left_inst_id,
                item.right_inst_id,
                item.left_kind,
                item.right_kind,
                item.template,
                item.description,
                "自定义" if item.is_custom else "默认",
            )
        ).lower()
        if query in haystack:
            result.append(item)
    return result


def custom_opportunities_file_path():
    return state_dir_path() / CUSTOM_OPPORTUNITIES_FILE_NAME


def load_custom_opportunities() -> list[ArbitrageOpportunityView]:
    target = custom_opportunities_file_path()
    if not target.exists():
        return []
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return []
    raw_items = payload.get("items") if isinstance(payload, dict) else []
    if not isinstance(raw_items, list):
        return []
    result: list[ArbitrageOpportunityView] = []
    for raw in raw_items:
        item = _deserialize_opportunity(raw)
        if item is not None:
            result.append(item)
    return result


def append_custom_opportunity(
    *,
    left_inst_id: str,
    right_inst_id: str,
    template: str,
    title: str = "",
    description: str = "",
) -> ArbitrageOpportunityView:
    item = build_custom_opportunity(
        left_inst_id=left_inst_id,
        right_inst_id=right_inst_id,
        template=template,
        title=title,
        description=description,
    )
    items = load_custom_opportunities()
    items.append(item)
    _save_custom_opportunities(items)
    return item


def remove_custom_opportunity(key: str) -> bool:
    current = load_custom_opportunities()
    filtered = [item for item in current if item.key != key]
    if len(filtered) == len(current):
        return False
    _save_custom_opportunities(filtered)
    return True


def build_custom_opportunity(
    *,
    left_inst_id: str,
    right_inst_id: str,
    template: str,
    title: str = "",
    description: str = "",
) -> ArbitrageOpportunityView:
    normalized_left = left_inst_id.strip().upper()
    normalized_right = right_inst_id.strip().upper()
    left_kind = infer_instrument_kind(normalized_left)
    right_kind = infer_instrument_kind(normalized_right)
    normalized_template = "roll" if template == "roll" else "professional"
    resolved_title = title.strip() or _default_custom_title(
        left_inst_id=normalized_left,
        right_inst_id=normalized_right,
        left_kind=left_kind,
        right_kind=right_kind,
        template=normalized_template,
    )
    resolved_description = description.strip() or _default_custom_description(
        left_inst_id=normalized_left,
        right_inst_id=normalized_right,
        left_kind=left_kind,
        right_kind=right_kind,
        template=normalized_template,
    )
    return ArbitrageOpportunityView(
        key=f"custom_{uuid4().hex[:12]}",
        title=resolved_title,
        left_inst_id=normalized_left,
        right_inst_id=normalized_right,
        left_kind=left_kind,
        right_kind=right_kind,
        template=normalized_template,
        description=resolved_description,
        is_custom=True,
    )


def infer_instrument_kind(inst_id: str) -> str:
    normalized = inst_id.strip().upper()
    if not normalized:
        return "未知"
    if normalized.endswith("-SWAP"):
        return "永续"
    parts = normalized.split("-")
    if len(parts) == 2:
        return "现货"
    if len(parts) == 3 and parts[2].isdigit() and len(parts[2]) == 6:
        return "交割"
    return "标的"


def _default_custom_title(
    *,
    left_inst_id: str,
    right_inst_id: str,
    left_kind: str,
    right_kind: str,
    template: str,
) -> str:
    base = left_inst_id.split("-", 1)[0].strip().upper() or "自定义"
    if template == "roll":
        return f"{base} 手动换月"
    return f"{base} 手动套利对"


def _default_custom_description(
    *,
    left_inst_id: str,
    right_inst_id: str,
    left_kind: str,
    right_kind: str,
    template: str,
) -> str:
    if template == "roll":
        return (
            f"自定义交割换月：{left_inst_id} ({left_kind}) -> {right_inst_id} ({right_kind})。"
            "可直接联动到右侧换月执行区。"
        )
    return (
        f"自定义专业套利对：{left_inst_id} ({left_kind}) <-> {right_inst_id} ({right_kind})。"
        "用于更灵活的手动看盘和后续双腿执行扩展。"
    )


def _serialize_opportunity(item: ArbitrageOpportunityView) -> dict[str, object]:
    return {
        "key": item.key,
        "title": item.title,
        "left_inst_id": item.left_inst_id,
        "right_inst_id": item.right_inst_id,
        "left_kind": item.left_kind,
        "right_kind": item.right_kind,
        "template": item.template,
        "description": item.description,
        "is_custom": item.is_custom,
    }


def _deserialize_opportunity(raw: object) -> ArbitrageOpportunityView | None:
    if not isinstance(raw, dict):
        return None
    left_inst_id = str(raw.get("left_inst_id") or "").strip().upper()
    right_inst_id = str(raw.get("right_inst_id") or "").strip().upper()
    if not left_inst_id or not right_inst_id:
        return None
    title = str(raw.get("title") or "").strip()
    template = str(raw.get("template") or "professional").strip().lower()
    key = str(raw.get("key") or "").strip() or f"custom_{uuid4().hex[:12]}"
    left_kind = str(raw.get("left_kind") or "").strip() or infer_instrument_kind(left_inst_id)
    right_kind = str(raw.get("right_kind") or "").strip() or infer_instrument_kind(right_inst_id)
    description = str(raw.get("description") or "").strip()
    return ArbitrageOpportunityView(
        key=key,
        title=title or _default_custom_title(
            left_inst_id=left_inst_id,
            right_inst_id=right_inst_id,
            left_kind=left_kind,
            right_kind=right_kind,
            template=template,
        ),
        left_inst_id=left_inst_id,
        right_inst_id=right_inst_id,
        left_kind=left_kind,
        right_kind=right_kind,
        template="roll" if template == "roll" else "professional",
        description=description or _default_custom_description(
            left_inst_id=left_inst_id,
            right_inst_id=right_inst_id,
            left_kind=left_kind,
            right_kind=right_kind,
            template=template,
        ),
        is_custom=True,
    )


def _save_custom_opportunities(items: list[ArbitrageOpportunityView]) -> None:
    target = custom_opportunities_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "items": [_serialize_opportunity(item) for item in items if item.is_custom],
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
