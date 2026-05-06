from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Literal
from uuid import uuid4


JournalBias = Literal["long", "short", "neutral", "unknown"]
JournalAction = Literal["open_long", "open_short", "observe", "unknown"]
JournalExtractionSource = Literal["local_rules", "ai_paste", "api"]
JournalEntryStatus = Literal["draft", "review", "confirmed", "monitoring", "archived"]

_COMMON_SYMBOLS = (
    "BTC",
    "ETH",
    "SOL",
    "BNB",
    "XRP",
    "DOGE",
    "ADA",
    "AVAX",
    "LINK",
    "LTC",
    "BCH",
    "DOT",
    "OP",
    "ARB",
)
_TIMEFRAME_PATTERN = re.compile(r"(?<![A-Za-z0-9])(\d{1,3})\s*([mMhHdDwW])\b")
_PRICE_PATTERN = re.compile(r"(?<![A-Za-z0-9])(\d+(?:\.\d+)?)(?!\s*[mMhHdDwW]\b)")


@dataclass(frozen=True)
class JournalExtractionResult:
    source: JournalExtractionSource
    symbol: str = ""
    inst_id: str = ""
    bias: JournalBias = "unknown"
    timeframes: tuple[str, ...] = ()
    entry_zone_price: Decimal | None = None
    entry_zone_text: str = ""
    trigger_text: str = ""
    invalidation_price: Decimal | None = None
    invalidation_text: str = ""
    planned_action: JournalAction = "unknown"
    position_size_text: str = ""
    summary: str = ""
    needs_review: bool = True
    review_questions: tuple[str, ...] = ()
    raw_payload: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "symbol": self.symbol,
            "inst_id": self.inst_id,
            "bias": self.bias,
            "timeframes": list(self.timeframes),
            "entry_zone_price": _decimal_to_text(self.entry_zone_price),
            "entry_zone_text": self.entry_zone_text,
            "trigger_text": self.trigger_text,
            "invalidation_price": _decimal_to_text(self.invalidation_price),
            "invalidation_text": self.invalidation_text,
            "planned_action": self.planned_action,
            "position_size_text": self.position_size_text,
            "summary": self.summary,
            "needs_review": self.needs_review,
            "review_questions": list(self.review_questions),
            "raw_payload": dict(self.raw_payload),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> JournalExtractionResult:
        return cls(
            source=_normalize_source(payload.get("source")),
            symbol=str(payload.get("symbol", "") or "").strip().upper(),
            inst_id=_normalize_inst_id(str(payload.get("inst_id", "") or "")),
            bias=_normalize_bias(payload.get("bias")),
            timeframes=_normalize_timeframes(payload.get("timeframes")),
            entry_zone_price=_parse_decimal(payload.get("entry_zone_price")),
            entry_zone_text=str(payload.get("entry_zone_text", "") or "").strip(),
            trigger_text=str(payload.get("trigger_text", "") or "").strip(),
            invalidation_price=_parse_decimal(payload.get("invalidation_price")),
            invalidation_text=str(payload.get("invalidation_text", "") or "").strip(),
            planned_action=_normalize_action(payload.get("planned_action")),
            position_size_text=str(payload.get("position_size_text", "") or "").strip(),
            summary=str(payload.get("summary", "") or "").strip(),
            needs_review=bool(payload.get("needs_review", True)),
            review_questions=tuple(
                str(item).strip()
                for item in payload.get("review_questions", []) or []
                if str(item).strip()
            ),
            raw_payload=dict(payload.get("raw_payload", {}) if isinstance(payload.get("raw_payload"), dict) else {}),
        )


@dataclass(frozen=True)
class JournalEntry:
    entry_id: str
    raw_text: str
    created_at: datetime
    updated_at: datetime
    attachments: tuple[str, ...] = ()
    status: JournalEntryStatus = "draft"
    extraction: JournalExtractionResult | None = None
    notes: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "entry_id": self.entry_id,
            "raw_text": self.raw_text,
            "created_at": _datetime_to_text(self.created_at),
            "updated_at": _datetime_to_text(self.updated_at),
            "attachments": list(self.attachments),
            "status": self.status,
            "extraction": self.extraction.to_dict() if self.extraction else None,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> JournalEntry:
        now = datetime.now(timezone.utc)
        raw_extraction = payload.get("extraction")
        extraction = (
            JournalExtractionResult.from_dict(raw_extraction)
            if isinstance(raw_extraction, dict)
            else None
        )
        return cls(
            entry_id=str(payload.get("entry_id", "") or "").strip() or uuid4().hex,
            raw_text=str(payload.get("raw_text", "") or ""),
            created_at=_parse_datetime(payload.get("created_at")) or now,
            updated_at=_parse_datetime(payload.get("updated_at")) or now,
            attachments=tuple(
                str(item).strip()
                for item in payload.get("attachments", []) or []
                if str(item).strip()
            ),
            status=_normalize_entry_status(payload.get("status")),
            extraction=extraction,
            notes=str(payload.get("notes", "") or ""),
        )


def create_journal_entry(
    raw_text: str,
    *,
    attachments: tuple[str, ...] = (),
    extraction: JournalExtractionResult | None = None,
    created_at: datetime | None = None,
) -> JournalEntry:
    now = created_at or datetime.now(timezone.utc)
    return JournalEntry(
        entry_id=uuid4().hex,
        raw_text=str(raw_text or ""),
        created_at=now,
        updated_at=now,
        attachments=attachments,
        extraction=extraction,
        status="review" if extraction else "draft",
    )


def extract_journal_locally(raw_text: str) -> JournalExtractionResult:
    text = _normalize_text(raw_text)
    symbol = _extract_symbol(text)
    inst_id = _infer_inst_id(symbol)
    timeframes = _extract_timeframes(text)
    bias = _extract_bias(text)
    planned_action = _extract_action(text, bias)
    entry_price, entry_text = _extract_entry_zone(text)
    invalidation_price, invalidation_text = _extract_invalidation(text)
    trigger_text = _extract_trigger_text(text, entry_price, invalidation_text)
    position_size_text = _extract_position_size(text)
    review_questions = _build_review_questions(
        text=text,
        symbol=symbol,
        entry_price=entry_price,
        invalidation_price=invalidation_price,
        position_size_text=position_size_text,
    )
    summary = _build_summary(
        inst_id=inst_id or symbol,
        bias=bias,
        timeframes=timeframes,
        entry_price=entry_price,
        trigger_text=trigger_text,
        invalidation_price=invalidation_price,
        planned_action=planned_action,
        position_size_text=position_size_text,
    )
    return JournalExtractionResult(
        source="local_rules",
        symbol=symbol,
        inst_id=inst_id,
        bias=bias,
        timeframes=timeframes,
        entry_zone_price=entry_price,
        entry_zone_text=entry_text,
        trigger_text=trigger_text,
        invalidation_price=invalidation_price,
        invalidation_text=invalidation_text,
        planned_action=planned_action,
        position_size_text=position_size_text,
        summary=summary,
        needs_review=bool(review_questions),
        review_questions=tuple(review_questions),
        raw_payload={"raw_text": raw_text},
    )


def parse_ai_extraction_paste(content: str) -> JournalExtractionResult:
    payload = _extract_json_payload(content)
    normalized = {
        "source": "ai_paste",
        "symbol": payload.get("symbol", ""),
        "inst_id": payload.get("inst_id") or payload.get("instrument") or "",
        "bias": payload.get("bias") or payload.get("direction") or "",
        "timeframes": payload.get("timeframes", []),
        "entry_zone_price": payload.get("entry_zone_price") or payload.get("entry_zone"),
        "entry_zone_text": payload.get("entry_zone_text", ""),
        "trigger_text": payload.get("trigger_text") or payload.get("trigger") or "",
        "invalidation_price": payload.get("invalidation_price") or payload.get("invalid_price"),
        "invalidation_text": payload.get("invalidation_text") or payload.get("invalid") or "",
        "planned_action": payload.get("planned_action") or payload.get("action") or "",
        "position_size_text": payload.get("position_size_text") or payload.get("position_size") or "",
        "summary": payload.get("summary", ""),
        "needs_review": payload.get("needs_review", True),
        "review_questions": payload.get("review_questions", []),
        "raw_payload": payload,
    }
    result = JournalExtractionResult.from_dict(normalized)
    if not result.inst_id:
        result = _replace_extraction(result, inst_id=_infer_inst_id(result.symbol))
    if not result.symbol and result.inst_id:
        result = _replace_extraction(result, symbol=result.inst_id.split("-")[0])
    return result


def build_ai_extraction_prompt(raw_text: str) -> str:
    return (
        "请把下面行情随笔提炼成严格 JSON，只输出 JSON，不要解释。\n"
        "不确定的字段填 null，并把 needs_review 设为 true。\n\n"
        "字段：symbol, inst_id, bias, timeframes, entry_zone_price, entry_zone_text, "
        "trigger_text, invalidation_price, invalidation_text, planned_action, "
        "position_size_text, summary, needs_review, review_questions\n\n"
        "取值约定：bias 只能是 long/short/neutral/unknown；planned_action 只能是 "
        "open_long/open_short/observe/unknown。\n\n"
        f"行情随笔：\n{raw_text}"
    )


def _replace_extraction(result: JournalExtractionResult, **changes: object) -> JournalExtractionResult:
    payload = result.to_dict()
    payload.update(changes)
    return JournalExtractionResult.from_dict(payload)


def _normalize_text(raw_text: str) -> str:
    return str(raw_text or "").replace("，", ",").replace("。", ".").replace("；", ";").strip()


def _extract_symbol(text: str) -> str:
    upper_text = text.upper()
    inst_match = re.search(r"\b([A-Z0-9]{2,12}-(?:USDT|USD)(?:-[A-Z]+)?)\b", upper_text)
    if inst_match:
        return inst_match.group(1).split("-")[0]
    for symbol in _COMMON_SYMBOLS:
        if re.search(rf"(?<![A-Z0-9]){re.escape(symbol)}(?![A-Z0-9])", upper_text):
            return symbol
    return ""


def _infer_inst_id(symbol: str) -> str:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        return ""
    if "-" in normalized:
        return _normalize_inst_id(normalized)
    return f"{normalized}-USDT-SWAP"


def _extract_timeframes(text: str) -> tuple[str, ...]:
    found: list[str] = []
    for amount, unit in _TIMEFRAME_PATTERN.findall(text):
        normalized_unit = unit.lower()
        timeframe = f"{int(amount)}{normalized_unit}" if normalized_unit == "m" else f"{int(amount)}{normalized_unit.upper()}"
        if timeframe not in found:
            found.append(timeframe)
    chinese_map = (("日线", "1D"), ("周线", "1W"), ("月线", "1M"), ("小时", "1H"))
    for keyword, timeframe in chinese_map:
        if keyword in text and timeframe not in found:
            found.append(timeframe)
    return tuple(found)


def _extract_bias(text: str) -> JournalBias:
    if any(keyword in text for keyword in ("看多", "偏多", "做多", "多头")):
        return "long"
    if any(keyword in text for keyword in ("看空", "偏空", "做空", "空头")):
        return "short"
    if any(keyword in text for keyword in ("震荡", "横盘", "观望")):
        return "neutral"
    return "unknown"


def _extract_action(text: str, bias: JournalBias) -> JournalAction:
    if any(keyword in text for keyword in ("做多", "开多", "多单")):
        return "open_long"
    if any(keyword in text for keyword in ("做空", "开空", "空单")):
        return "open_short"
    if bias == "long" and "准备" in text:
        return "open_long"
    if bias == "short" and "准备" in text:
        return "open_short"
    if bias == "neutral":
        return "observe"
    return "unknown"


def _extract_entry_zone(text: str) -> tuple[Decimal | None, str]:
    price_token = r"(\d+(?:\.\d+)?)(?!\s*[mMhHdDwW]\b)"
    patterns = (
        rf"(?:回踩|支撑|接近|靠近|到|在)\s*{price_token}\s*(附近|一带|左右)?",
        rf"{price_token}\s*(附近|一带|左右)?[^.;。；]*(?:回踩|不破|企稳|止跌|做多|做空)",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        price = _parse_decimal(match.group(1))
        if price is None:
            continue
        return price, match.group(0).strip(" ,.;")
    return None, ""


def _extract_invalidation(text: str) -> tuple[Decimal | None, str]:
    patterns = (
        r"(?:有效)?(?:跌破|失守|下破|低于)\s*(\d+(?:\.\d+)?)[^.;。；]*(?:作废|失效|放弃)?",
        r"(\d+(?:\.\d+)?)[^.;。；]*(?:作废|失效|放弃)",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        price = _parse_decimal(match.group(1))
        if price is None:
            continue
        return price, match.group(0).strip(" ,.;")
    return None, ""


def _extract_trigger_text(text: str, entry_price: Decimal | None, invalidation_text: str) -> str:
    chunks = [chunk.strip(" ,.;") for chunk in re.split(r"[.;]", text) if chunk.strip(" ,.;")]
    for chunk in chunks:
        if invalidation_text and invalidation_text in chunk:
            continue
        if entry_price is not None and _decimal_to_text(entry_price) in chunk:
            return chunk
        if any(keyword in chunk for keyword in ("回踩", "不破", "止跌", "企稳", "突破")):
            return chunk
    return ""


def _extract_position_size(text: str) -> str:
    for keyword in ("轻仓", "小仓", "半仓", "重仓", "满仓", "试一笔", "试仓"):
        if keyword in text:
            return keyword
    percent_match = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
    if percent_match:
        return f"{percent_match.group(1)}%"
    return ""


def _build_review_questions(
    *,
    text: str,
    symbol: str,
    entry_price: Decimal | None,
    invalidation_price: Decimal | None,
    position_size_text: str,
) -> list[str]:
    questions: list[str] = []
    if not symbol:
        questions.append("需要确认标的。")
    if entry_price is not None and any(keyword in text for keyword in ("附近", "一带", "左右")):
        questions.append("需要确认关键价位附近的容差范围。")
    if any(keyword in text for keyword in ("止跌形态", "企稳", "回踩不破", "不破")):
        questions.append("需要确认形态/企稳的具体触发标准。")
    if position_size_text in {"轻仓", "小仓", "试一笔", "试仓"}:
        questions.append("需要确认轻仓对应的仓位比例。")
    if invalidation_price is None:
        questions.append("需要确认失效条件。")
    return questions


def _build_summary(
    *,
    inst_id: str,
    bias: JournalBias,
    timeframes: tuple[str, ...],
    entry_price: Decimal | None,
    trigger_text: str,
    invalidation_price: Decimal | None,
    planned_action: JournalAction,
    position_size_text: str,
) -> str:
    parts: list[str] = []
    if inst_id:
        parts.append(inst_id)
    if timeframes:
        parts.append("/".join(timeframes))
    if bias != "unknown":
        parts.append({"long": "偏多", "short": "偏空", "neutral": "震荡/观望"}[bias])
    if entry_price is not None:
        parts.append(f"{_decimal_to_text(entry_price)}附近观察")
    if trigger_text:
        parts.append(trigger_text)
    action_label = {"open_long": "准备做多", "open_short": "准备做空", "observe": "继续观察", "unknown": ""}[planned_action]
    if action_label:
        parts.append(f"{position_size_text}{action_label}" if position_size_text else action_label)
    if invalidation_price is not None:
        parts.append(f"跌破/触及 {_decimal_to_text(invalidation_price)} 失效")
    return "，".join(parts)


def _extract_json_payload(content: str) -> dict[str, object]:
    text = str(content or "").strip()
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.S | re.I)
    candidate = fenced.group(1) if fenced else text
    try:
        payload = json.loads(candidate)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            raise ValueError("未找到可解析的 JSON 对象。")
        payload = json.loads(text[start : end + 1])
    if not isinstance(payload, dict):
        raise ValueError("AI 提炼结果必须是 JSON 对象。")
    return payload


def _normalize_source(value: object) -> JournalExtractionSource:
    normalized = str(value or "").strip()
    if normalized in {"local_rules", "ai_paste", "api"}:
        return normalized  # type: ignore[return-value]
    return "local_rules"


def _normalize_bias(value: object) -> JournalBias:
    normalized = str(value or "").strip().lower()
    mapping = {
        "long": "long",
        "多": "long",
        "看多": "long",
        "short": "short",
        "空": "short",
        "看空": "short",
        "neutral": "neutral",
        "震荡": "neutral",
        "observe": "neutral",
    }
    return mapping.get(normalized, "unknown")  # type: ignore[return-value]


def _normalize_action(value: object) -> JournalAction:
    normalized = str(value or "").strip().lower()
    mapping = {
        "open_long": "open_long",
        "long": "open_long",
        "做多": "open_long",
        "open_short": "open_short",
        "short": "open_short",
        "做空": "open_short",
        "observe": "observe",
        "watch": "observe",
        "观望": "observe",
    }
    return mapping.get(normalized, "unknown")  # type: ignore[return-value]


def _normalize_entry_status(value: object) -> JournalEntryStatus:
    normalized = str(value or "").strip()
    if normalized in {"draft", "review", "confirmed", "monitoring", "archived"}:
        return normalized  # type: ignore[return-value]
    return "draft"


def _normalize_timeframes(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        raw_items = [item.strip() for item in re.split(r"[,/，、\s]+", value) if item.strip()]
    elif isinstance(value, list | tuple):
        raw_items = [str(item).strip() for item in value if str(item).strip()]
    else:
        raw_items = []
    normalized: list[str] = []
    for item in raw_items:
        match = re.fullmatch(r"(\d{1,3})\s*([mMhHdDwW])", item)
        timeframe = item
        if match:
            amount, unit = match.groups()
            timeframe = f"{int(amount)}{unit.lower()}" if unit.lower() == "m" else f"{int(amount)}{unit.upper()}"
        if timeframe not in normalized:
            normalized.append(timeframe)
    return tuple(normalized)


def _normalize_inst_id(value: str) -> str:
    return str(value or "").strip().upper().replace("_", "-").replace("/", "-")


def _parse_decimal(value: object) -> Decimal | None:
    text = str(value or "").strip()
    if not text or text.lower() == "none" or text.lower() == "null":
        return None
    match = _PRICE_PATTERN.search(text)
    decimal_text = match.group(1) if match else text
    try:
        return Decimal(decimal_text)
    except (InvalidOperation, ValueError):
        return None


def _decimal_to_text(value: Decimal | None) -> str:
    if value is None:
        return ""
    text = format(value.normalize(), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _parse_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _datetime_to_text(value: datetime) -> str:
    target = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return target.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
