from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Literal
from uuid import uuid4


JournalBias = Literal["long", "short", "neutral", "mixed", "unknown"]
JournalAction = Literal["open_long", "open_short", "observe", "none", "unknown"]
JournalExtractionSource = Literal["local_rules", "ai_paste", "api"]
JournalEntryStatus = Literal["draft", "review", "confirmed", "monitoring", "archived"]
JournalRecordType = Literal["trade_plan", "market_view", "research_hypothesis", "post_trade_review", "unknown"]

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
class JournalCondition:
    type: str = "unknown"
    text: str = ""
    value: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "type": self.type,
            "text": self.text,
            "value": self.value,
        }

    @classmethod
    def from_value(cls, value: object) -> JournalCondition:
        if isinstance(value, dict):
            return cls(
                type=str(value.get("type", "") or "unknown").strip() or "unknown",
                text=str(value.get("text", "") or "").strip(),
                value=str(value.get("value", "") or "").strip(),
            )
        return cls(text=str(value or "").strip()) if str(value or "").strip() else cls()


@dataclass(frozen=True)
class JournalHypothesis:
    type: str = "unknown"
    statement: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "type": self.type,
            "statement": self.statement,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object] | None) -> JournalHypothesis:
        payload = payload if isinstance(payload, dict) else {}
        return cls(
            type=str(payload.get("type", "") or "unknown").strip() or "unknown",
            statement=str(payload.get("statement", "") or "").strip(),
        )


@dataclass(frozen=True)
class JournalExecutionPlan:
    intended_action: JournalAction = "unknown"
    position_style: str = "unknown"
    position_size_note: str = ""
    entry_idea: str = ""
    trigger_conditions: tuple[JournalCondition, ...] = ()
    invalidation_conditions: tuple[JournalCondition, ...] = ()
    targets: tuple[JournalCondition, ...] = ()
    risk_notes: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "intended_action": self.intended_action,
            "position_style": self.position_style,
            "position_size_note": self.position_size_note,
            "entry_idea": self.entry_idea,
            "trigger_conditions": [item.to_dict() for item in self.trigger_conditions],
            "invalidation_conditions": [item.to_dict() for item in self.invalidation_conditions],
            "targets": [item.to_dict() for item in self.targets],
            "risk_notes": self.risk_notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object] | None) -> JournalExecutionPlan:
        payload = payload if isinstance(payload, dict) else {}
        return cls(
            intended_action=_normalize_action(payload.get("intended_action")),
            position_style=str(payload.get("position_style", "") or "unknown").strip() or "unknown",
            position_size_note=str(payload.get("position_size_note", "") or "").strip(),
            entry_idea=str(payload.get("entry_idea", "") or "").strip(),
            trigger_conditions=_conditions_from_value(payload.get("trigger_conditions")),
            invalidation_conditions=_conditions_from_value(payload.get("invalidation_conditions")),
            targets=_conditions_from_value(payload.get("targets")),
            risk_notes=str(payload.get("risk_notes", "") or "").strip(),
        )


@dataclass(frozen=True)
class JournalObservation:
    key_levels: tuple[JournalCondition, ...] = ()
    structure_notes: str = ""
    volatility_notes: str = ""
    program_disagreement_note: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "key_levels": [item.to_dict() for item in self.key_levels],
            "structure_notes": self.structure_notes,
            "volatility_notes": self.volatility_notes,
            "program_disagreement_note": self.program_disagreement_note,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object] | None) -> JournalObservation:
        payload = payload if isinstance(payload, dict) else {}
        return cls(
            key_levels=_conditions_from_value(payload.get("key_levels")),
            structure_notes=str(payload.get("structure_notes", "") or "").strip(),
            volatility_notes=str(payload.get("volatility_notes", "") or "").strip(),
            program_disagreement_note=str(payload.get("program_disagreement_note", "") or "").strip(),
        )


@dataclass(frozen=True)
class JournalVerificationPlan:
    status: str = "pending"
    verification_type: str = "unknown"
    success_criteria: tuple[str, ...] = ()
    failure_criteria: tuple[str, ...] = ()
    review_windows: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "verification_type": self.verification_type,
            "success_criteria": list(self.success_criteria),
            "failure_criteria": list(self.failure_criteria),
            "review_windows": list(self.review_windows),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object] | None) -> JournalVerificationPlan:
        payload = payload if isinstance(payload, dict) else {}
        return cls(
            status=str(payload.get("status", "") or "pending").strip() or "pending",
            verification_type=str(payload.get("verification_type", "") or "unknown").strip() or "unknown",
            success_criteria=_string_tuple(payload.get("success_criteria")),
            failure_criteria=_string_tuple(payload.get("failure_criteria")),
            review_windows=_string_tuple(payload.get("review_windows")),
        )


@dataclass(frozen=True)
class JournalResearchSample:
    record_type: JournalRecordType = "unknown"
    symbol: str = ""
    timeframe: str = ""
    related_timeframes: tuple[str, ...] = ()
    market_phase: str = "unknown"
    title: str = ""
    raw_summary: str = ""
    bias: JournalBias = "unknown"
    confidence: float = 0.0
    priority: str = "unknown"
    hypothesis: JournalHypothesis = field(default_factory=JournalHypothesis)
    execution_plan: JournalExecutionPlan = field(default_factory=JournalExecutionPlan)
    observation: JournalObservation = field(default_factory=JournalObservation)
    verification_plan: JournalVerificationPlan = field(default_factory=JournalVerificationPlan)
    method_tags: tuple[str, ...] = ()
    review_questions: tuple[str, ...] = ()
    attachments: tuple[str, ...] = ()
    notes_for_me: str = ""
    raw_payload: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "record_type": self.record_type,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "related_timeframes": list(self.related_timeframes),
            "market_phase": self.market_phase,
            "title": self.title,
            "raw_summary": self.raw_summary,
            "bias": self.bias,
            "confidence": self.confidence,
            "priority": self.priority,
            "hypothesis": self.hypothesis.to_dict(),
            "execution_plan": self.execution_plan.to_dict(),
            "observation": self.observation.to_dict(),
            "verification_plan": self.verification_plan.to_dict(),
            "method_tags": list(self.method_tags),
            "review_questions": list(self.review_questions),
            "attachments": list(self.attachments),
            "notes_for_me": self.notes_for_me,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> JournalResearchSample:
        timeframe = _first_timeframe(payload.get("timeframe"))
        related_timeframes = _normalize_timeframes(payload.get("related_timeframes"))
        return cls(
            record_type=_normalize_record_type(payload.get("record_type")),
            symbol=_normalize_inst_id(str(payload.get("symbol", "") or "")),
            timeframe=timeframe,
            related_timeframes=related_timeframes,
            market_phase=str(payload.get("market_phase", "") or "unknown").strip() or "unknown",
            title=str(payload.get("title", "") or "").strip(),
            raw_summary=str(payload.get("raw_summary", "") or "").strip(),
            bias=_normalize_bias(payload.get("bias")),
            confidence=_parse_float(payload.get("confidence")),
            priority=str(payload.get("priority", "") or "unknown").strip() or "unknown",
            hypothesis=JournalHypothesis.from_dict(
                payload.get("hypothesis") if isinstance(payload.get("hypothesis"), dict) else None
            ),
            execution_plan=JournalExecutionPlan.from_dict(
                payload.get("execution_plan") if isinstance(payload.get("execution_plan"), dict) else None
            ),
            observation=JournalObservation.from_dict(
                payload.get("observation") if isinstance(payload.get("observation"), dict) else None
            ),
            verification_plan=JournalVerificationPlan.from_dict(
                payload.get("verification_plan") if isinstance(payload.get("verification_plan"), dict) else None
            ),
            method_tags=_string_tuple(payload.get("method_tags")),
            review_questions=_string_tuple(payload.get("review_questions")),
            attachments=_string_tuple(payload.get("attachments")),
            notes_for_me=str(payload.get("notes_for_me", "") or "").strip(),
            raw_payload=dict(payload),
        )


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
    if _looks_like_research_sample(payload):
        return _parse_research_sample_extraction(payload)
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


def parse_research_sample_paste(content: str) -> JournalResearchSample:
    return JournalResearchSample.from_dict(_extract_json_payload(content))


def build_research_sample_prompt(raw_text: str) -> str:
    return (
        "请把下面这段交易/行情随笔，提炼成严格 JSON。\n\n"
        "要求：\n"
        "1. 只输出 JSON，不要解释，不要加代码块。\n"
        "2. 所有字段必须保留。\n"
        "3. 不确定的字段填空字符串、空数组、空对象，或 \"unknown\"。\n"
        "4. 不要编造价格、周期、仓位、目标位、止损位。\n"
        "5. 如果缺少关键条件，review_questions 必须逐条列出来。\n"
        "6. 如果一句话可能被理解成指标信号和价格行为形态，必须在 review_questions 中要求澄清。\n\n"
        "取值约定：\n"
        "- record_type: trade_plan / market_view / research_hypothesis / post_trade_review / unknown\n"
        "- bias: long / short / neutral / mixed / unknown\n"
        "- market_phase: trend_up / trend_down / range / breakout / breakdown / unknown\n"
        "- hypothesis.type: conditional_trade / directional_view / pattern_hypothesis / volatility_hypothesis / method_hypothesis / unknown\n"
        "- execution_plan.intended_action: open_long / open_short / observe / none / unknown\n"
        "- execution_plan.position_style: light / normal / heavy / unknown\n"
        "- verification_plan.status: pending\n"
        "- verification_plan.verification_type: market_outcome / method_validation / signal_validation / unknown\n"
        "- priority: low / medium / high / unknown\n\n"
        "JSON 结构：\n"
        "{\n"
        "  \"record_type\": \"\",\n"
        "  \"symbol\": \"\",\n"
        "  \"timeframe\": \"\",\n"
        "  \"related_timeframes\": [],\n"
        "  \"market_phase\": \"\",\n"
        "  \"title\": \"\",\n"
        "  \"raw_summary\": \"\",\n"
        "  \"bias\": \"\",\n"
        "  \"confidence\": 0,\n"
        "  \"priority\": \"\",\n"
        "  \"hypothesis\": {\"type\": \"\", \"statement\": \"\"},\n"
        "  \"execution_plan\": {\n"
        "    \"intended_action\": \"\",\n"
        "    \"position_style\": \"\",\n"
        "    \"position_size_note\": \"\",\n"
        "    \"entry_idea\": \"\",\n"
        "    \"trigger_conditions\": [],\n"
        "    \"invalidation_conditions\": [],\n"
        "    \"targets\": [],\n"
        "    \"risk_notes\": \"\"\n"
        "  },\n"
        "  \"observation\": {\n"
        "    \"key_levels\": [],\n"
        "    \"structure_notes\": \"\",\n"
        "    \"volatility_notes\": \"\",\n"
        "    \"program_disagreement_note\": \"\"\n"
        "  },\n"
        "  \"verification_plan\": {\n"
        "    \"status\": \"pending\",\n"
        "    \"verification_type\": \"\",\n"
        "    \"success_criteria\": [],\n"
        "    \"failure_criteria\": [],\n"
        "    \"review_windows\": []\n"
        "  },\n"
        "  \"method_tags\": [],\n"
        "  \"review_questions\": [],\n"
        "  \"attachments\": [],\n"
        "  \"notes_for_me\": \"\"\n"
        "}\n\n"
        f"原始随笔：\n{raw_text}"
    )


def build_ai_extraction_prompt(raw_text: str) -> str:
    return build_research_sample_prompt(raw_text)
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


def _looks_like_research_sample(payload: dict[str, object]) -> bool:
    return any(
        key in payload
        for key in (
            "record_type",
            "hypothesis",
            "execution_plan",
            "verification_plan",
            "method_tags",
        )
    )


def _normalize_record_type(value: object) -> JournalRecordType:
    normalized = str(value or "").strip()
    if normalized in {"trade_plan", "market_view", "research_hypothesis", "post_trade_review"}:
        return normalized  # type: ignore[return-value]
    return "unknown"


def _conditions_from_value(value: object) -> tuple[JournalCondition, ...]:
    if not isinstance(value, list | tuple):
        return ()
    return tuple(
        condition
        for condition in (JournalCondition.from_value(item) for item in value)
        if condition.type != "unknown" or condition.text or condition.value
    )


def _string_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        items = [value]
    elif isinstance(value, list | tuple):
        items = [str(item) for item in value]
    else:
        items = []
    return tuple(item.strip() for item in items if item.strip())


def _first_timeframe(value: object) -> str:
    timeframes = _normalize_timeframes(value)
    return timeframes[0] if timeframes else ""


def _parse_float(value: object) -> float:
    try:
        return float(str(value or "0").strip())
    except ValueError:
        return 0.0


def _parse_research_sample_extraction(payload: dict[str, object]) -> JournalExtractionResult:
    sample = JournalResearchSample.from_dict(payload)
    raw_symbol = sample.symbol
    inst_id = _infer_inst_id(raw_symbol)
    symbol = raw_symbol.split("-")[0] if "-" in raw_symbol else raw_symbol
    timeframes = _normalize_timeframes([sample.timeframe, *sample.related_timeframes])
    trigger_conditions = [item.to_dict() for item in sample.execution_plan.trigger_conditions]
    invalidation_conditions = [item.to_dict() for item in sample.execution_plan.invalidation_conditions]
    entry_price, entry_text = _first_condition_price_and_text(trigger_conditions)
    invalidation_price, invalidation_text = _first_condition_price_and_text(invalidation_conditions)
    review_questions = sample.review_questions
    if not review_questions:
        review_questions = _research_sample_review_questions(sample.to_dict(), trigger_conditions, invalidation_conditions)
    trigger_text = "; ".join(item.get("text", "") for item in trigger_conditions if item.get("text"))
    if not trigger_text:
        trigger_text = sample.execution_plan.entry_idea
    return JournalExtractionResult(
        source="ai_paste",
        symbol=symbol,
        inst_id=inst_id,
        bias=sample.bias,
        timeframes=timeframes,
        entry_zone_price=entry_price,
        entry_zone_text=entry_text,
        trigger_text=trigger_text,
        invalidation_price=invalidation_price,
        invalidation_text=invalidation_text,
        planned_action=sample.execution_plan.intended_action,
        position_size_text=sample.execution_plan.position_size_note,
        summary=sample.raw_summary or sample.title or sample.hypothesis.statement,
        needs_review=bool(review_questions),
        review_questions=review_questions,
        raw_payload=sample.to_dict(),
    )

    execution_plan = payload.get("execution_plan")
    if not isinstance(execution_plan, dict):
        execution_plan = {}
    hypothesis = payload.get("hypothesis")
    if not isinstance(hypothesis, dict):
        hypothesis = {}

    raw_symbol = str(payload.get("symbol", "") or "").strip().upper()
    inst_id = _infer_inst_id(raw_symbol)
    symbol = raw_symbol.split("-")[0] if "-" in raw_symbol else raw_symbol
    related_timeframes = payload.get("related_timeframes")
    if not isinstance(related_timeframes, list):
        related_timeframes = []
    timeframes = _normalize_timeframes([payload.get("timeframe", ""), *related_timeframes])
    trigger_conditions = _normalize_condition_list(execution_plan.get("trigger_conditions"))
    invalidation_conditions = _normalize_condition_list(execution_plan.get("invalidation_conditions"))
    entry_price, entry_text = _first_condition_price_and_text(trigger_conditions)
    invalidation_price, invalidation_text = _first_condition_price_and_text(invalidation_conditions)
    review_questions = tuple(
        str(item).strip()
        for item in payload.get("review_questions", []) or []
        if str(item).strip()
    )
    if not review_questions:
        review_questions = _research_sample_review_questions(payload, trigger_conditions, invalidation_conditions)
    summary = str(payload.get("raw_summary", "") or "").strip() or str(payload.get("title", "") or "").strip()
    trigger_text = "；".join(item.get("text", "") for item in trigger_conditions if item.get("text"))
    if not trigger_text:
        trigger_text = str(execution_plan.get("entry_idea", "") or "").strip()
    return JournalExtractionResult(
        source="ai_paste",
        symbol=symbol,
        inst_id=inst_id,
        bias=_normalize_bias(payload.get("bias")),
        timeframes=timeframes,
        entry_zone_price=entry_price,
        entry_zone_text=entry_text,
        trigger_text=trigger_text,
        invalidation_price=invalidation_price,
        invalidation_text=invalidation_text,
        planned_action=_normalize_action(execution_plan.get("intended_action")),
        position_size_text=str(execution_plan.get("position_size_note", "") or "").strip(),
        summary=summary or str(hypothesis.get("statement", "") or "").strip(),
        needs_review=bool(review_questions),
        review_questions=review_questions,
        raw_payload=dict(payload),
    )


def _normalize_condition_list(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    conditions: list[dict[str, str]] = []
    for item in value:
        if isinstance(item, dict):
            conditions.append(
                {
                    "type": str(item.get("type", "") or "").strip(),
                    "text": str(item.get("text", "") or "").strip(),
                    "value": str(item.get("value", "") or "").strip(),
                }
            )
        elif str(item).strip():
            conditions.append({"type": "unknown", "text": str(item).strip(), "value": ""})
    return conditions


def _first_condition_price_and_text(conditions: list[dict[str, str]]) -> tuple[Decimal | None, str]:
    for condition in conditions:
        value = _parse_decimal(condition.get("value"))
        text = condition.get("text", "")
        if value is None:
            value = _parse_decimal(text)
        if value is not None or text:
            return value, text
    return None, ""


def _research_sample_review_questions(
    payload: dict[str, object],
    trigger_conditions: list[dict[str, str]],
    invalidation_conditions: list[dict[str, str]],
) -> tuple[str, ...]:
    questions: list[str] = []
    if not str(payload.get("symbol", "") or "").strip():
        questions.append("需要确认标的。")
    if not str(payload.get("timeframe", "") or "").strip():
        questions.append("需要确认主分析周期。")
    record_type = str(payload.get("record_type", "") or "").strip()
    if record_type == "trade_plan" and not trigger_conditions:
        questions.append("交易计划需要补充触发条件。")
    if record_type in {"trade_plan", "market_view"} and not invalidation_conditions:
        questions.append("需要补充失效条件或判错标准。")
    verification_plan = payload.get("verification_plan")
    if isinstance(verification_plan, dict):
        if not verification_plan.get("success_criteria"):
            questions.append("需要补充验证成功标准。")
        if not verification_plan.get("failure_criteria"):
            questions.append("需要补充验证失败标准。")
    return tuple(questions)


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
        "mixed": "mixed",
        "分歧": "mixed",
        "混合": "mixed",
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
        "none": "none",
        "无": "none",
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
