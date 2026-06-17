from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from okx_quant.btc_market_replay import ReplayValidation, build_replay_validation, replay_validation_payload
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path


@dataclass(frozen=True)
class EmailAnalysisRecord:
    archive_meta_path: Path
    generated_at: str
    symbol: str
    asset: str
    direction: str
    stance: str
    score: int
    confidence: str
    timeframe: str
    analysis_candle_ts: int | None
    entry_price: Decimal | None
    summary: str
    report_path: Path | None = None


@dataclass(frozen=True)
class EmailValidationResult:
    archive_meta_path: Path
    generated_at: str
    symbol: str
    asset: str
    direction: str
    stance: str
    score: int
    confidence: str
    timeframe: str
    analysis_candle_ts: int | None
    entry_price: Decimal | None
    summary: str
    validation: ReplayValidation


def load_email_analysis_records(
    *,
    base_dir: Path | str | None = None,
    archive_dir: Path | None = None,
    symbols: Iterable[str] | None = None,
    limit: int = 0,
) -> list[EmailAnalysisRecord]:
    target_dir = archive_dir or (analysis_report_dir_path(_to_path(base_dir)) / "email_archives")
    if not target_dir.exists():
        return []
    symbol_filter = {str(item).strip().upper() for item in (symbols or ()) if str(item).strip()}
    meta_paths = sorted(target_dir.glob("multi_coin_market_digest_email_*.json"))
    if limit > 0:
        meta_paths = meta_paths[-limit:]
    records: list[EmailAnalysisRecord] = []
    for meta_path in meta_paths:
        metadata = _load_json_dict(meta_path)
        if not metadata:
            continue
        if not _archive_metadata_is_delivered(metadata):
            continue
        digest_payload = _resolve_digest_payload(metadata, meta_path=meta_path)
        if not digest_payload:
            continue
        viewpoints = _viewpoint_map(metadata, digest_payload)
        generated_at = str(
            metadata.get("generated_at")
            or digest_payload.get("generated_at")
            or metadata.get("archived_at")
            or ""
        ).strip()
        report_path = _resolve_report_path(metadata.get("report_path"), meta_path=meta_path)
        for analysis_payload in _analysis_payloads(digest_payload):
            symbol = str(analysis_payload.get("symbol", "") or "").strip().upper()
            if not symbol:
                continue
            if symbol_filter and symbol not in symbol_filter:
                continue
            primary = _primary_timeframe_payload(analysis_payload)
            if primary is None:
                continue
            viewpoint = viewpoints.get(symbol) or _derive_viewpoint_from_payload(analysis_payload)
            records.append(
                EmailAnalysisRecord(
                    archive_meta_path=meta_path,
                    generated_at=generated_at,
                    symbol=symbol,
                    asset=symbol.split("-")[0].upper(),
                    direction=str(analysis_payload.get("direction", "") or "").strip().lower(),
                    stance=str(viewpoint.get("stance", "") or "").strip() or "暂观望",
                    score=_to_int(analysis_payload.get("score")),
                    confidence=str(analysis_payload.get("confidence", "") or "").strip(),
                    timeframe=str(primary.get("timeframe", "") or "").strip() or "1H",
                    analysis_candle_ts=_to_optional_int(primary.get("candle_ts")),
                    entry_price=_to_decimal(primary.get("last_close")),
                    summary=str(viewpoint.get("summary", "") or "").strip(),
                    report_path=report_path,
                )
            )
    records.sort(key=lambda item: (item.generated_at, item.symbol))
    return records


def refresh_email_validation_report(
    *,
    base_dir: Path | str | None = None,
    archive_dir: Path | None = None,
    out_dir: Path | None = None,
    symbols: Iterable[str] | None = None,
    archive_limit: int = 60,
    windows_hours: tuple[int, ...] = (4, 12, 24, 72),
    client: OkxRestClient | None = None,
) -> dict[str, object] | None:
    records = load_email_analysis_records(
        base_dir=base_dir,
        archive_dir=archive_dir,
        symbols=symbols,
        limit=archive_limit,
    )
    if not records:
        return None
    results = validate_email_analysis_records(records, client=client, windows_hours=windows_hours)
    payload = build_email_validation_report_payload(results, windows_hours=windows_hours)
    save_email_validation_report(payload, base_dir=base_dir, out_dir=out_dir)
    return payload


def validate_email_analysis_records(
    records: Iterable[EmailAnalysisRecord],
    *,
    client: OkxRestClient | None = None,
    windows_hours: tuple[int, ...] = (4, 12, 24, 72),
) -> list[EmailValidationResult]:
    resolved_client = client or OkxRestClient()
    max_window_hours = max((hours for hours in windows_hours if hours > 0), default=0)
    results: list[EmailValidationResult] = []
    for record in records:
        future_candles = []
        if record.analysis_candle_ts is not None and max_window_hours > 0:
            try:
                future_candles = resolved_client.get_candles_history_range(
                    record.symbol,
                    record.timeframe,
                    start_ts=record.analysis_candle_ts + 1,
                    end_ts=record.analysis_candle_ts + (max_window_hours * 3_600_000),
                    limit=0,
                    preload_count=0,
                )
            except Exception:
                future_candles = []
            future_candles = [item for item in future_candles if int(item.ts) > int(record.analysis_candle_ts)]
        validation = build_replay_validation(
            direction=record.direction,
            timeframe=record.timeframe,
            entry_price=record.entry_price,
            analysis_candle_ts=record.analysis_candle_ts,
            future_candles=future_candles,
            timeframe_ms=_timeframe_ms(record.timeframe),
            windows_hours=windows_hours,
        )
        results.append(
            EmailValidationResult(
                archive_meta_path=record.archive_meta_path,
                generated_at=record.generated_at,
                symbol=record.symbol,
                asset=record.asset,
                direction=record.direction,
                stance=record.stance,
                score=record.score,
                confidence=record.confidence,
                timeframe=record.timeframe,
                analysis_candle_ts=record.analysis_candle_ts,
                entry_price=record.entry_price,
                summary=record.summary,
                validation=validation,
            )
        )
    return results


def build_email_validation_report_payload(
    results: Iterable[EmailValidationResult],
    *,
    windows_hours: tuple[int, ...],
) -> dict[str, object]:
    rows = list(results)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "windows_hours": list(windows_hours),
        "overall": _summary_payload(rows),
        "actionable": _summary_payload([item for item in rows if item.stance != "暂观望"]),
        "by_symbol": {
            symbol: _summary_payload([item for item in rows if item.symbol == symbol])
            for symbol in sorted({item.symbol for item in rows})
        },
        "by_stance": {
            stance: _summary_payload([item for item in rows if item.stance == stance])
            for stance in sorted({item.stance for item in rows})
        },
        "details": [_detail_payload(item, windows_hours=windows_hours) for item in rows],
    }


def save_email_validation_report(
    payload: dict[str, object],
    *,
    base_dir: Path | str | None = None,
    out_dir: Path | None = None,
) -> dict[str, Path]:
    target_dir = out_dir or (analysis_report_dir_path(_to_path(base_dir)) / "validation")
    target_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base_name = f"multi_coin_email_validation_{stamp}"
    json_path = target_dir / f"{base_name}.json"
    csv_path = target_dir / f"{base_name}.csv"
    md_path = target_dir / f"{base_name}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_validation_csv(csv_path, payload)
    md_path.write_text(_build_validation_markdown(payload, json_path=json_path, csv_path=csv_path), encoding="utf-8")
    return {"json": json_path, "csv": csv_path, "md": md_path}


def load_latest_email_validation_payload(
    *,
    base_dir: Path | str | None = None,
    validation_dir: Path | None = None,
) -> dict[str, object] | None:
    target_dir = validation_dir or (analysis_report_dir_path(_to_path(base_dir)) / "validation")
    if not target_dir.exists():
        return None
    candidates = sorted(target_dir.glob("multi_coin_email_validation_*.json"))
    if not candidates:
        return None
    payload = _load_json_dict(candidates[-1])
    return payload or None


def build_recent_email_validation_summary(
    payload: dict[str, object],
    *,
    recent_email_limit: int = 20,
) -> dict[str, object] | None:
    details = payload.get("details")
    if not isinstance(details, list) or not details:
        return None
    detail_rows = [item for item in details if isinstance(item, dict)]
    if not detail_rows:
        return None
    recent_rows = _recent_email_detail_rows(detail_rows, recent_email_limit=recent_email_limit)
    if not recent_rows:
        return None
    actionable_rows = [item for item in recent_rows if str(item.get("stance", "") or "").strip() != "暂观望"]
    by_symbol = {
        symbol: _summary_payload_from_details([item for item in recent_rows if str(item.get("symbol", "") or "").strip() == symbol])
        for symbol in sorted({str(item.get("symbol", "") or "").strip() for item in recent_rows if str(item.get("symbol", "") or "").strip()})
    }
    summary = {
        "generated_at": str(payload.get("generated_at", "") or "").strip(),
        "recent_email_limit": recent_email_limit,
        "email_count": _unique_archive_count(recent_rows),
        "sample_count": len(recent_rows),
        "actionable_sample_count": len(actionable_rows),
        "overall": _summary_payload_from_details(recent_rows),
        "actionable": _summary_payload_from_details(actionable_rows),
        "by_symbol": by_symbol,
    }
    summary["highlights"] = _build_recent_summary_highlights(summary)
    return summary


def _archive_metadata_is_delivered(metadata: dict[str, object]) -> bool:
    delivery_status = str(metadata.get("delivery_status", "") or "").strip().lower()
    if not delivery_status:
        return True
    return delivery_status in {"sent", "released"}


def _build_recent_summary_highlights(summary: dict[str, object]) -> dict[str, object]:
    overall = summary.get("overall") if isinstance(summary.get("overall"), dict) else {}
    actionable = summary.get("actionable") if isinstance(summary.get("actionable"), dict) else {}
    by_symbol = summary.get("by_symbol") if isinstance(summary.get("by_symbol"), dict) else {}
    ranked = [(symbol, item) for symbol, item in by_symbol.items() if isinstance(item, dict) and int(item.get("completed", 0) or 0) > 0]
    best = None
    worst = None
    if ranked:
        best_symbol, best_item = max(
            ranked,
            key=lambda pair: (
                float(pair[1].get("hit_rate_pct", 0) or 0),
                int(pair[1].get("completed", 0) or 0),
                float(pair[1].get("avg_return_24h_pct", 0) or 0),
                pair[0],
            ),
        )
        worst_symbol, worst_item = min(
            ranked,
            key=lambda pair: (
                float(pair[1].get("hit_rate_pct", 0) or 0),
                -int(pair[1].get("completed", 0) or 0),
                float(pair[1].get("avg_return_24h_pct", 0) or 0),
                pair[0],
            ),
        )
        best = {"symbol": best_symbol, "asset": best_symbol.split("-")[0].upper(), "summary": dict(best_item)}
        worst = {"symbol": worst_symbol, "asset": worst_symbol.split("-")[0].upper(), "summary": dict(worst_item)}
    return {
        "best_symbol": best,
        "worst_symbol": worst,
        "notable_change": _build_notable_change_text(
            overall=overall,
            actionable=actionable,
            best=best,
            worst=worst,
        ),
    }


def _build_notable_change_text(
    *,
    overall: dict[str, object],
    actionable: dict[str, object],
    best: dict[str, object] | None,
    worst: dict[str, object] | None,
) -> str:
    overall_pending = int(overall.get("pending", 0) or 0)
    overall_completed = int(overall.get("completed", 0) or 0)
    actionable_hit = float(actionable.get("hit_rate_pct", 0) or 0)
    overall_hit = float(overall.get("hit_rate_pct", 0) or 0)
    if overall_pending > overall_completed:
        return "最近样本仍有较多待验证结果，先继续积累，再看结论是否稳定。"
    if best is not None and worst is not None:
        best_hit = float(best["summary"].get("hit_rate_pct", 0) or 0)
        worst_hit = float(worst["summary"].get("hit_rate_pct", 0) or 0)
        if best["symbol"] != worst["symbol"] and (best_hit - worst_hit) >= 40:
            return f"币种分化明显，{best['asset']} 明显强于 {worst['asset']}，后续可以重点跟踪分化是否持续。"
    if actionable_hit >= overall_hit + 15:
        return "明确观点样本明显优于总体样本，说明当前筛选条件是有价值的。"
    if actionable_hit + 15 <= overall_hit:
        return "明确观点样本并没有跑赢总体样本，后续需要继续收紧观点触发条件。"
    avg_24h = overall.get("avg_return_24h_pct")
    if isinstance(avg_24h, (int, float)) and float(avg_24h) < 0:
        return "最近 24H 平均回报转弱，说明短线观点稳定性还不够。"
    return "最近整体表现还在可接受区间，更值得继续观察样本扩充后的稳定性。"


def _resolve_digest_payload(metadata: dict[str, object], *, meta_path: Path) -> dict[str, object] | None:
    digest_payload = metadata.get("digest_payload")
    if isinstance(digest_payload, dict):
        return digest_payload
    report_path = _resolve_report_path(metadata.get("report_path"), meta_path=meta_path)
    if report_path is None or not report_path.exists():
        return None
    payload = _load_json_dict(report_path)
    return payload or None


def _viewpoint_map(metadata: dict[str, object], digest_payload: dict[str, object]) -> dict[str, dict[str, object]]:
    rows = metadata.get("viewpoints")
    if isinstance(rows, list):
        mapped: dict[str, dict[str, object]] = {}
        for item in rows:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol", "") or "").strip().upper()
            if symbol:
                mapped[symbol] = item
        if mapped:
            return mapped
    mapped = {}
    for analysis_payload in _analysis_payloads(digest_payload):
        symbol = str(analysis_payload.get("symbol", "") or "").strip().upper()
        if symbol:
            mapped[symbol] = _derive_viewpoint_from_payload(analysis_payload)
    return mapped


def _derive_viewpoint_from_payload(analysis_payload: dict[str, object]) -> dict[str, object]:
    tf4h = _find_timeframe_payload(analysis_payload, "4H")
    tf1h = _find_timeframe_payload(analysis_payload, "1H")
    direction = str(analysis_payload.get("direction", "") or "").strip().lower()
    score = _to_int(analysis_payload.get("score"))
    confidence = str(analysis_payload.get("confidence", "") or "").strip()
    reasons = analysis_payload.get("reason")
    lead_reason = ""
    if isinstance(reasons, list) and reasons:
        lead_reason = str(reasons[0] or "").strip()
    if tf4h and tf1h and str(tf4h.get("direction", "")).lower() == str(tf1h.get("direction", "")).lower():
        tf_direction = str(tf4h.get("direction", "") or "").strip().lower()
        if tf_direction == "long":
            return {
                "stance": "优先做多",
                "summary": f"4H 与 1H 同向偏多，综合分数 {score}，置信度 {confidence}。",
            }
        if tf_direction == "short":
            return {
                "stance": "优先做空",
                "summary": f"4H 与 1H 同向偏空，综合分数 {score}，置信度 {confidence}。",
            }
    if direction == "long" and score >= 4:
        return {"stance": "偏多跟踪", "summary": f"综合分数 {score}，置信度 {confidence}，核心依据：{lead_reason}"}
    if direction == "short" and score <= -4:
        return {"stance": "偏空跟踪", "summary": f"综合分数 {score}，置信度 {confidence}，核心依据：{lead_reason}"}
    return {"stance": "暂观望", "summary": f"综合分数 {score}，置信度 {confidence}，核心依据：{lead_reason}"}


def _analysis_payloads(digest_payload: dict[str, object]) -> list[dict[str, object]]:
    rows = digest_payload.get("analyses")
    if not isinstance(rows, list):
        return []
    return [item for item in rows if isinstance(item, dict)]


def _primary_timeframe_payload(analysis_payload: dict[str, object]) -> dict[str, object] | None:
    for timeframe in ("1H", "4H", "1D"):
        row = _find_timeframe_payload(analysis_payload, timeframe)
        if row is not None and _to_optional_int(row.get("candle_ts")) is not None and _to_decimal(row.get("last_close")) is not None:
            return row
    timeframes = analysis_payload.get("timeframes")
    if not isinstance(timeframes, list):
        return None
    for row in timeframes:
        if not isinstance(row, dict):
            continue
        if _to_optional_int(row.get("candle_ts")) is not None and _to_decimal(row.get("last_close")) is not None:
            return row
    return None


def _find_timeframe_payload(analysis_payload: dict[str, object], timeframe: str) -> dict[str, object] | None:
    timeframes = analysis_payload.get("timeframes")
    if not isinstance(timeframes, list):
        return None
    for row in timeframes:
        if isinstance(row, dict) and str(row.get("timeframe", "") or "").strip().upper() == timeframe.upper():
            return row
    return None


def _summary_payload(rows: list[EmailValidationResult]) -> dict[str, object]:
    total = len(rows)
    effective = sum(1 for item in rows if item.validation.verdict == "effective")
    partial = sum(1 for item in rows if item.validation.verdict == "partially_effective")
    mixed = sum(1 for item in rows if item.validation.verdict == "mixed")
    invalid = sum(1 for item in rows if item.validation.verdict == "invalid") + mixed
    pending = sum(1 for item in rows if item.validation.verdict not in {"effective", "partially_effective", "invalid", "mixed"})
    completed = max(total - pending, 0)
    hit_rate = ((effective + partial) / completed * 100.0) if completed > 0 else 0.0
    return {
        "samples": total,
        "completed": completed,
        "effective": effective,
        "partial": partial,
        "invalid": invalid,
        "pending": pending,
        "hit_rate_pct": round(hit_rate, 2),
        "avg_return_4h_pct": _average_window_return(rows, 4),
        "avg_return_12h_pct": _average_window_return(rows, 12),
        "avg_return_24h_pct": _average_window_return(rows, 24),
        "avg_return_72h_pct": _average_window_return(rows, 72),
    }


def _summary_payload_from_details(rows: list[dict[str, object]]) -> dict[str, object]:
    total = len(rows)
    effective = 0
    partial = 0
    mixed = 0
    invalid = 0
    pending = 0
    returns_by_hours: dict[int, list[float]] = {4: [], 12: [], 24: [], 72: []}
    for row in rows:
        validation = row.get("validation")
        verdict = str(validation.get("verdict", "") or "").strip() if isinstance(validation, dict) else ""
        if verdict == "effective":
            effective += 1
        elif verdict == "partially_effective":
            partial += 1
        elif verdict == "mixed":
            mixed += 1
        elif verdict == "invalid":
            invalid += 1
        else:
            pending += 1
        for hours in returns_by_hours:
            value = row.get(f"return_{hours}h_pct")
            if isinstance(value, (int, float)):
                returns_by_hours[hours].append(float(value))
    invalid += mixed
    completed = max(total - pending, 0)
    hit_rate = ((effective + partial) / completed * 100.0) if completed > 0 else 0.0
    return {
        "samples": total,
        "completed": completed,
        "effective": effective,
        "partial": partial,
        "invalid": invalid,
        "pending": pending,
        "hit_rate_pct": round(hit_rate, 2),
        "avg_return_4h_pct": _average_float_values(returns_by_hours[4]),
        "avg_return_12h_pct": _average_float_values(returns_by_hours[12]),
        "avg_return_24h_pct": _average_float_values(returns_by_hours[24]),
        "avg_return_72h_pct": _average_float_values(returns_by_hours[72]),
    }


def _average_window_return(rows: list[EmailValidationResult], hours: int) -> float | None:
    values: list[float] = []
    for item in rows:
        value = _window_return_value(item.validation, hours)
        if value is not None:
            values.append(value)
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _average_float_values(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _window_return_value(validation: ReplayValidation, hours: int) -> float | None:
    for item in validation.windows:
        if int(item.hours) != int(hours):
            continue
        if item.return_pct is None:
            return None
        return float(item.return_pct)
    return None


def _detail_payload(item: EmailValidationResult, *, windows_hours: tuple[int, ...]) -> dict[str, object]:
    payload = {
        "archive_meta_path": str(item.archive_meta_path),
        "generated_at": item.generated_at,
        "symbol": item.symbol,
        "asset": item.asset,
        "direction": item.direction,
        "stance": item.stance,
        "score": item.score,
        "confidence": item.confidence,
        "timeframe": item.timeframe,
        "analysis_candle_ts": item.analysis_candle_ts,
        "entry_price": _decimal_text(item.entry_price),
        "summary": item.summary,
        "validation": replay_validation_payload(item.validation),
    }
    for hours in windows_hours:
        payload[f"return_{hours}h_pct"] = _window_return_value(item.validation, hours)
    return payload


def _write_validation_csv(path: Path, payload: dict[str, object]) -> None:
    details = payload.get("details")
    if not isinstance(details, list):
        path.write_text("", encoding="utf-8")
        return
    window_keys = sorted(
        {
            key
            for item in details
            if isinstance(item, dict)
            for key in item.keys()
            if key.startswith("return_") and key.endswith("_pct")
        }
    )
    fieldnames = [
        "generated_at",
        "symbol",
        "asset",
        "direction",
        "stance",
        "score",
        "confidence",
        "timeframe",
        "analysis_candle_ts",
        "entry_price",
        "summary",
        "verdict",
        "status",
        "max_favorable_excursion_pct",
        "max_adverse_excursion_pct",
        *window_keys,
        "archive_meta_path",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in details:
            if not isinstance(item, dict):
                continue
            validation = item.get("validation")
            row = {key: item.get(key) for key in fieldnames}
            if isinstance(validation, dict):
                row["verdict"] = validation.get("verdict")
                row["status"] = validation.get("status")
                row["max_favorable_excursion_pct"] = validation.get("max_favorable_excursion_pct")
                row["max_adverse_excursion_pct"] = validation.get("max_adverse_excursion_pct")
            writer.writerow(row)


def _build_validation_markdown(payload: dict[str, object], *, json_path: Path, csv_path: Path) -> str:
    overall = payload.get("overall") if isinstance(payload.get("overall"), dict) else {}
    actionable = payload.get("actionable") if isinstance(payload.get("actionable"), dict) else {}
    lines = [
        "# Multi-Coin Email Validation",
        "",
        f"- Generated at (UTC): {payload.get('generated_at', '-')}",
        f"- JSON: `{json_path}`",
        f"- CSV: `{csv_path}`",
        f"- Review windows: {', '.join(f'{item}H' for item in payload.get('windows_hours', []))}",
        "",
        "## Overall",
        "",
        _summary_markdown_line(overall),
        "",
        "## Actionable Only",
        "",
        _summary_markdown_line(actionable),
        "",
        "## By Symbol",
        "",
    ]
    by_symbol = payload.get("by_symbol")
    if isinstance(by_symbol, dict) and by_symbol:
        for symbol, summary in by_symbol.items():
            if isinstance(summary, dict):
                lines.append(f"- {symbol}: {_summary_markdown_line(summary)}")
    else:
        lines.append("- None")
    lines.extend(["", "## By Stance", ""])
    by_stance = payload.get("by_stance")
    if isinstance(by_stance, dict) and by_stance:
        for stance, summary in by_stance.items():
            if isinstance(summary, dict):
                lines.append(f"- {stance}: {_summary_markdown_line(summary)}")
    else:
        lines.append("- None")
    return "\n".join(lines).rstrip() + "\n"


def _summary_markdown_line(summary: dict[str, object]) -> str:
    if not summary:
        return "samples=0"
    return (
        f"samples={summary.get('samples', 0)} | completed={summary.get('completed', 0)} | "
        f"hit_rate={summary.get('hit_rate_pct', 0)}% | effective={summary.get('effective', 0)} | "
        f"partial={summary.get('partial', 0)} | invalid={summary.get('invalid', 0)} | "
        f"pending={summary.get('pending', 0)} | avg_24h={summary.get('avg_return_24h_pct', '-')}"
    )


def _resolve_report_path(value: object, *, meta_path: Path) -> Path | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    candidate = Path(raw)
    if candidate.is_absolute():
        return candidate
    if candidate.exists():
        return candidate.resolve()
    return (meta_path.parent / candidate).resolve()


def _recent_email_detail_rows(rows: list[dict[str, object]], *, recent_email_limit: int) -> list[dict[str, object]]:
    if recent_email_limit <= 0:
        return list(rows)
    ordered = sorted(
        rows,
        key=lambda item: (
            str(item.get("generated_at", "") or "").strip(),
            str(item.get("archive_meta_path", "") or "").strip(),
            str(item.get("symbol", "") or "").strip(),
        ),
    )
    selected_archives: list[str] = []
    for row in reversed(ordered):
        archive_key = str(row.get("archive_meta_path", "") or "").strip()
        if not archive_key or archive_key in selected_archives:
            continue
        selected_archives.append(archive_key)
        if len(selected_archives) >= recent_email_limit:
            break
    keep = set(selected_archives)
    return [row for row in ordered if str(row.get("archive_meta_path", "") or "").strip() in keep]


def _unique_archive_count(rows: list[dict[str, object]]) -> int:
    return len({str(row.get("archive_meta_path", "") or "").strip() for row in rows if str(row.get("archive_meta_path", "") or "").strip()})


def _load_json_dict(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _timeframe_ms(timeframe: str) -> int:
    normalized = str(timeframe or "").strip().upper()
    if normalized.endswith("M"):
        return int(normalized[:-1]) * 60_000
    if normalized.endswith("H"):
        return int(normalized[:-1]) * 3_600_000
    if normalized.endswith("D"):
        return int(normalized[:-1]) * 86_400_000
    if normalized.endswith("W"):
        return int(normalized[:-1]) * 7 * 86_400_000
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _to_optional_int(value: object) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _to_decimal(value: object) -> Decimal | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return Decimal(raw)
    except Exception:
        return None


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _to_path(value: Path | str | None) -> Path | None:
    if value is None:
        return None
    return Path(value)
