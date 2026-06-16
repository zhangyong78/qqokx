from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path


getcontext().prec = 28

ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = Path(r"D:\qqokx_data\state\backtest_history.json")
REPORT_PREFIX = ROOT / "reports" / "doge_dynamic_long_s652_s655_review_latest"
SNAPSHOT_IDS = ("S652", "S653", "S654", "S655")


@dataclass
class SnapshotSummary:
    snapshot_id: str
    created_at: str
    symbol: str
    bar: str
    start: str
    end: str
    ema_period: int
    ema_type: str
    trend_ema_period: int
    trend_ema_type: str
    entry_reference_period: int
    entry_reference_type: str
    atr_period: int
    stop_loss_atr: str
    max_entries: int
    break_even_trigger_r: str
    rules_text: str
    trades: int
    win_rate: Decimal
    pnl: Decimal
    profit_factor: Decimal
    max_drawdown: Decimal
    avg_r: Decimal
    total_fees: Decimal
    fee_ratio_net_pct: Decimal
    return_drawdown_ratio: Decimal
    export_path: str
    operations_path: str


@dataclass
class StressSummary:
    snapshot_id: str
    top_5_share_pct: Decimal
    top_10_share_pct: Decimal
    top_20_share_pct: Decimal
    remove_top_5_pnl: Decimal
    remove_top_5_pf: Decimal
    remove_top_5_dd: Decimal
    remove_top_5_return_dd: Decimal
    remove_top_10_pnl: Decimal
    remove_top_10_pf: Decimal
    remove_top_10_dd: Decimal
    remove_top_10_return_dd: Decimal
    remove_top_20_pnl: Decimal
    remove_top_20_pf: Decimal
    remove_top_20_dd: Decimal
    remove_top_20_return_dd: Decimal
    positive_months: int
    negative_months: int
    total_months: int


def _dec(value: str | int | float | Decimal) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _fmt_decimal(value: Decimal, digits: int = 4) -> str:
    quant = Decimal("1").scaleb(-digits)
    return format(value.quantize(quant, rounding=ROUND_HALF_UP), "f")


def _fmt_pct(value: Decimal, digits: int = 2) -> str:
    return f"{_fmt_decimal(value, digits)}%"


def _fmt_r_token(value: Decimal) -> str:
    if value == value.to_integral():
        return format(value.quantize(Decimal("1")), "f")
    normalized = value.normalize()
    return format(normalized, "f").rstrip("0").rstrip(".")


def _rules_text(rules: list[dict[str, object]]) -> str:
    parts: list[str] = []
    for rule in rules:
        trigger_r = _fmt_r_token(_dec(rule["trigger_r"]))
        action = str(rule["action"])
        if action == "break_even":
            parts.append(f"{trigger_r}R保本")
            continue
        lock_r = _fmt_r_token(_dec(rule["lock_r"]))
        trail_mode = str(rule.get("trail_mode") or "none")
        trail_every_r = rule.get("trail_every_r")
        trail_add_r = rule.get("trail_add_r")
        if trail_mode == "step" and trail_every_r is not None and trail_add_r is not None:
            parts.append(
                f"{trigger_r}R锁{lock_r}R后每{_fmt_r_token(_dec(trail_every_r))}R移"
                f"{_fmt_r_token(_dec(trail_add_r))}R"
            )
        else:
            parts.append(f"{trigger_r}R锁{lock_r}R")
    return " / ".join(parts)


def _load_records() -> dict[str, dict[str, object]]:
    payload = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    records = {}
    for record in payload["records"]:
        sid = record.get("snapshot_id")
        if sid in SNAPSHOT_IDS:
            records[sid] = record
    missing = [sid for sid in SNAPSHOT_IDS if sid not in records]
    if missing:
        raise RuntimeError(f"missing snapshots: {missing}")
    return records


def _read_exit_trades(operations_path: Path) -> list[dict[str, object]]:
    exits: list[dict[str, object]] = []
    with operations_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row["group"] != "trade" or row["action"] != "exit":
                continue
            exits.append(
                {
                    "datetime": row["datetime"],
                    "pnl": _dec(row["pnl"]),
                }
            )
    return exits


def _calc_metrics(exits: list[dict[str, object]], initial_capital: Decimal) -> dict[str, object]:
    total = sum((_dec(item["pnl"]) for item in exits), Decimal("0"))
    gross_profit = sum((_dec(item["pnl"]) for item in exits if _dec(item["pnl"]) > 0), Decimal("0"))
    gross_loss = sum((-_dec(item["pnl"]) for item in exits if _dec(item["pnl"]) < 0), Decimal("0"))
    profit_factor = gross_profit / gross_loss if gross_loss else Decimal("0")
    equity = initial_capital
    peak = initial_capital
    max_drawdown = Decimal("0")
    monthly = defaultdict(Decimal)
    wins = 0
    losses = 0
    for item in exits:
        pnl = _dec(item["pnl"])
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
        monthly[str(item["datetime"])[:7]] += pnl
    return {
        "trades": len(exits),
        "wins": wins,
        "losses": losses,
        "pnl": total,
        "avg_pnl": total / Decimal(len(exits)),
        "profit_factor": profit_factor,
        "max_drawdown": max_drawdown,
        "return_drawdown_ratio": total / max_drawdown if max_drawdown else Decimal("0"),
        "positive_months": sum(1 for value in monthly.values() if value > 0),
        "negative_months": sum(1 for value in monthly.values() if value < 0),
        "total_months": len(monthly),
    }


def _remove_top_winners(exits: list[dict[str, object]], top_n: int) -> tuple[list[dict[str, object]], Decimal]:
    positives = [item for item in exits if _dec(item["pnl"]) > 0]
    positives.sort(key=lambda item: _dec(item["pnl"]), reverse=True)
    winners_to_remove = positives[:top_n]
    counts = Counter((str(item["datetime"]), _dec(item["pnl"])) for item in winners_to_remove)
    remaining: list[dict[str, object]] = []
    for item in exits:
        key = (str(item["datetime"]), _dec(item["pnl"]))
        if counts[key] > 0:
            counts[key] -= 1
            continue
        remaining.append(item)
    removed_sum = sum((_dec(item["pnl"]) for item in winners_to_remove), Decimal("0"))
    return remaining, removed_sum


def _build_summaries() -> tuple[list[SnapshotSummary], list[StressSummary]]:
    records = _load_records()
    snapshots: list[SnapshotSummary] = []
    stresses: list[StressSummary] = []
    for snapshot_id in SNAPSHOT_IDS:
        record = records[snapshot_id]
        config = record["config"]
        report = record["report"]
        export_path = Path(record["export_path"])
        operations_path = export_path.with_suffix(".operations.csv")
        exits = _read_exit_trades(operations_path)
        initial_capital = _dec(config["backtest_initial_capital"])
        metrics = _calc_metrics(exits, initial_capital)
        total_fees = _dec(report["total_fees"])
        pnl = _dec(report["total_pnl"])
        snapshots.append(
            SnapshotSummary(
                snapshot_id=snapshot_id,
                created_at=str(record["created_at"]),
                symbol=str(config["inst_id"]),
                bar=str(config["bar"]),
                start=str(record["start_ts"]),
                end=str(record["end_ts"]),
                ema_period=int(config["ema_period"]),
                ema_type=str(config["ema_type"]),
                trend_ema_period=int(config["trend_ema_period"]),
                trend_ema_type=str(config["trend_ema_type"]),
                entry_reference_period=int(config["entry_reference_ema_period"]),
                entry_reference_type=str(config["entry_reference_ema_type"]),
                atr_period=int(config["atr_period"]),
                stop_loss_atr=str(config["atr_stop_multiplier"]),
                max_entries=int(config["max_entries_per_trend"]),
                break_even_trigger_r=str(config["dynamic_break_even_trigger_r"]),
                rules_text=_rules_text(config["dynamic_protection_rules"]),
                trades=int(report["total_trades"]),
                win_rate=_dec(report["win_rate"]),
                pnl=pnl,
                profit_factor=_dec(report["profit_factor"]),
                max_drawdown=_dec(report["max_drawdown"]),
                avg_r=_dec(report["average_r_multiple"]),
                total_fees=total_fees,
                fee_ratio_net_pct=(total_fees / pnl) * Decimal("100"),
                return_drawdown_ratio=pnl / _dec(report["max_drawdown"]),
                export_path=str(export_path),
                operations_path=str(operations_path),
            )
        )
        positive_pnls = sorted((_dec(item["pnl"]) for item in exits if _dec(item["pnl"]) > 0), reverse=True)
        top_5_sum = sum(positive_pnls[:5], Decimal("0"))
        top_10_sum = sum(positive_pnls[:10], Decimal("0"))
        top_20_sum = sum(positive_pnls[:20], Decimal("0"))
        remove_5, _ = _remove_top_winners(exits, 5)
        remove_10, _ = _remove_top_winners(exits, 10)
        remove_20, _ = _remove_top_winners(exits, 20)
        remove_5_metrics = _calc_metrics(remove_5, initial_capital)
        remove_10_metrics = _calc_metrics(remove_10, initial_capital)
        remove_20_metrics = _calc_metrics(remove_20, initial_capital)
        stresses.append(
            StressSummary(
                snapshot_id=snapshot_id,
                top_5_share_pct=(top_5_sum / pnl) * Decimal("100"),
                top_10_share_pct=(top_10_sum / pnl) * Decimal("100"),
                top_20_share_pct=(top_20_sum / pnl) * Decimal("100"),
                remove_top_5_pnl=_dec(remove_5_metrics["pnl"]),
                remove_top_5_pf=_dec(remove_5_metrics["profit_factor"]),
                remove_top_5_dd=_dec(remove_5_metrics["max_drawdown"]),
                remove_top_5_return_dd=_dec(remove_5_metrics["return_drawdown_ratio"]),
                remove_top_10_pnl=_dec(remove_10_metrics["pnl"]),
                remove_top_10_pf=_dec(remove_10_metrics["profit_factor"]),
                remove_top_10_dd=_dec(remove_10_metrics["max_drawdown"]),
                remove_top_10_return_dd=_dec(remove_10_metrics["return_drawdown_ratio"]),
                remove_top_20_pnl=_dec(remove_20_metrics["pnl"]),
                remove_top_20_pf=_dec(remove_20_metrics["profit_factor"]),
                remove_top_20_dd=_dec(remove_20_metrics["max_drawdown"]),
                remove_top_20_return_dd=_dec(remove_20_metrics["return_drawdown_ratio"]),
                positive_months=int(metrics["positive_months"]),
                negative_months=int(metrics["negative_months"]),
                total_months=int(metrics["total_months"]),
            )
        )
    return snapshots, stresses


def _write_csv(snapshots: list[SnapshotSummary], stresses: list[StressSummary]) -> None:
    stress_map = {item.snapshot_id: item for item in stresses}
    with REPORT_PREFIX.with_suffix(".csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "snapshot_id",
                "ema",
                "trend_ema",
                "atr_period",
                "stop_loss_atr",
                "break_even_trigger_r",
                "rules_text",
                "trades",
                "win_rate_pct",
                "pnl",
                "profit_factor",
                "max_drawdown",
                "avg_r",
                "total_fees",
                "fee_ratio_net_pct",
                "return_drawdown_ratio",
                "top_5_share_pct",
                "top_10_share_pct",
                "top_20_share_pct",
                "remove_top_5_pnl",
                "remove_top_5_pf",
                "remove_top_5_dd",
                "remove_top_10_pnl",
                "remove_top_10_pf",
                "remove_top_10_dd",
                "remove_top_20_pnl",
                "remove_top_20_pf",
                "remove_top_20_dd",
                "positive_months",
                "negative_months",
                "total_months",
            ]
        )
        for snapshot in snapshots:
            stress = stress_map[snapshot.snapshot_id]
            writer.writerow(
                [
                    snapshot.snapshot_id,
                    f"{snapshot.ema_type.upper()}{snapshot.ema_period}",
                    f"{snapshot.trend_ema_type.upper()}{snapshot.trend_ema_period}",
                    snapshot.atr_period,
                    snapshot.stop_loss_atr,
                    snapshot.break_even_trigger_r,
                    snapshot.rules_text,
                    snapshot.trades,
                    _fmt_decimal(snapshot.win_rate, 4),
                    _fmt_decimal(snapshot.pnl, 4),
                    _fmt_decimal(snapshot.profit_factor, 4),
                    _fmt_decimal(snapshot.max_drawdown, 4),
                    _fmt_decimal(snapshot.avg_r, 4),
                    _fmt_decimal(snapshot.total_fees, 4),
                    _fmt_decimal(snapshot.fee_ratio_net_pct, 4),
                    _fmt_decimal(snapshot.return_drawdown_ratio, 4),
                    _fmt_decimal(stress.top_5_share_pct, 4),
                    _fmt_decimal(stress.top_10_share_pct, 4),
                    _fmt_decimal(stress.top_20_share_pct, 4),
                    _fmt_decimal(stress.remove_top_5_pnl, 4),
                    _fmt_decimal(stress.remove_top_5_pf, 4),
                    _fmt_decimal(stress.remove_top_5_dd, 4),
                    _fmt_decimal(stress.remove_top_10_pnl, 4),
                    _fmt_decimal(stress.remove_top_10_pf, 4),
                    _fmt_decimal(stress.remove_top_10_dd, 4),
                    _fmt_decimal(stress.remove_top_20_pnl, 4),
                    _fmt_decimal(stress.remove_top_20_pf, 4),
                    _fmt_decimal(stress.remove_top_20_dd, 4),
                    stress.positive_months,
                    stress.negative_months,
                    stress.total_months,
                ]
            )


def _write_json(snapshots: list[SnapshotSummary], stresses: list[StressSummary]) -> None:
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "snapshots": [
            {
                **asdict(snapshot),
                "win_rate": str(snapshot.win_rate),
                "pnl": str(snapshot.pnl),
                "profit_factor": str(snapshot.profit_factor),
                "max_drawdown": str(snapshot.max_drawdown),
                "avg_r": str(snapshot.avg_r),
                "total_fees": str(snapshot.total_fees),
                "fee_ratio_net_pct": str(snapshot.fee_ratio_net_pct),
                "return_drawdown_ratio": str(snapshot.return_drawdown_ratio),
            }
            for snapshot in snapshots
        ],
        "stress": [
            {
                **asdict(stress),
                "top_5_share_pct": str(stress.top_5_share_pct),
                "top_10_share_pct": str(stress.top_10_share_pct),
                "top_20_share_pct": str(stress.top_20_share_pct),
                "remove_top_5_pnl": str(stress.remove_top_5_pnl),
                "remove_top_5_pf": str(stress.remove_top_5_pf),
                "remove_top_5_dd": str(stress.remove_top_5_dd),
                "remove_top_5_return_dd": str(stress.remove_top_5_return_dd),
                "remove_top_10_pnl": str(stress.remove_top_10_pnl),
                "remove_top_10_pf": str(stress.remove_top_10_pf),
                "remove_top_10_dd": str(stress.remove_top_10_dd),
                "remove_top_10_return_dd": str(stress.remove_top_10_return_dd),
                "remove_top_20_pnl": str(stress.remove_top_20_pnl),
                "remove_top_20_pf": str(stress.remove_top_20_pf),
                "remove_top_20_dd": str(stress.remove_top_20_dd),
                "remove_top_20_return_dd": str(stress.remove_top_20_return_dd),
            }
            for stress in stresses
        ],
    }
    REPORT_PREFIX.with_suffix(".json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _write_markdown(snapshots: list[SnapshotSummary], stresses: list[StressSummary]) -> None:
    stress_map = {item.snapshot_id: item for item in stresses}
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: list[str] = [
        "# DOGE 动态做多 S652-S655 复盘",
        "",
        f"生成时间：{generated_at}",
        "",
        "本轮只做研究，不改最佳参数包、UI 默认参数或实盘口径。",
        "",
        "## 锁死口径",
        "",
        "- 标的：`DOGE-USDT-SWAP`",
        "- 周期：`1H`",
        "- 回测区间：`2020-07-10 11:00:00` -> `2026-06-16 16:00:00`",
        "- 固定风险金：`100U/笔`",
        "- 初始资金：`10000U`",
        "- 复利：关闭",
        "- 手续费：Maker `0.0150%` / Taker `0.0360%`",
        "- 本轮只比较 `EMA5 / 趋势EMA13 / ATR10 / SL1 / 每波2次` 不变时，不同保本触发R的差异。",
        "",
        "## 参数与主指标",
        "",
        "| 快照 | 参数 | 交易数 | 胜率 | PnL | PF | DD | AvgR | 手续费占净盈亏 | PnL/DD |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for snapshot in snapshots:
        lines.append(
            "| "
            f"{snapshot.snapshot_id} | "
            f"`{snapshot.rules_text}` | "
            f"{snapshot.trades} | "
            f"{_fmt_pct(snapshot.win_rate, 2)} | "
            f"{_fmt_decimal(snapshot.pnl, 4)} | "
            f"{_fmt_decimal(snapshot.profit_factor, 4)} | "
            f"{_fmt_decimal(snapshot.max_drawdown, 4)} | "
            f"{_fmt_decimal(snapshot.avg_r, 4)} | "
            f"{_fmt_pct(snapshot.fee_ratio_net_pct, 2)} | "
            f"{_fmt_decimal(snapshot.return_drawdown_ratio, 4)} |"
        )
    lines.extend(
        [
            "",
            "## 尾部依赖压力测试",
            "",
            "| 快照 | Top5盈利占总PnL | Top10盈利占总PnL | Top20盈利占总PnL | 去掉Top5后PnL | 去掉Top10后PnL | 去掉Top20后PnL |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for snapshot in snapshots:
        stress = stress_map[snapshot.snapshot_id]
        lines.append(
            "| "
            f"{snapshot.snapshot_id} | "
            f"{_fmt_pct(stress.top_5_share_pct, 2)} | "
            f"{_fmt_pct(stress.top_10_share_pct, 2)} | "
            f"{_fmt_pct(stress.top_20_share_pct, 2)} | "
            f"{_fmt_decimal(stress.remove_top_5_pnl, 4)} | "
            f"{_fmt_decimal(stress.remove_top_10_pnl, 4)} | "
            f"{_fmt_decimal(stress.remove_top_20_pnl, 4)} |"
        )
    lines.extend(
        [
            "",
            "## 压力后质量",
            "",
            "| 快照 | 去掉Top5后PF | 去掉Top5后DD | 去掉Top10后PF | 去掉Top10后DD | 去掉Top20后PF | 去掉Top20后DD | 正收益月 / 负收益月 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for snapshot in snapshots:
        stress = stress_map[snapshot.snapshot_id]
        lines.append(
            "| "
            f"{snapshot.snapshot_id} | "
            f"{_fmt_decimal(stress.remove_top_5_pf, 4)} | "
            f"{_fmt_decimal(stress.remove_top_5_dd, 4)} | "
            f"{_fmt_decimal(stress.remove_top_10_pf, 4)} | "
            f"{_fmt_decimal(stress.remove_top_10_dd, 4)} | "
            f"{_fmt_decimal(stress.remove_top_20_pf, 4)} | "
            f"{_fmt_decimal(stress.remove_top_20_dd, 4)} | "
            f"`{stress.positive_months} / {stress.negative_months}` |"
        )
    lines.extend(
        [
            "",
            "## 判断",
            "",
            "- `S653` 是这组里最均衡的版本：收益最高、PF 最高、回撤还比 `S652` 更低，胜率也从 `25.53%` 抬到 `34.56%`。",
            "- `S652` 更像纯趋势版：`AvgR` 最高，但低胜率和更大回撤让持有体验最差。",
            "- `S654` 的高胜率是用更密的交易和更重的手续费换来的：虽然回撤更低，但 `PnL`、`PF`、`AvgR` 都不如 `S653`，手续费占净盈亏已接近一半。",
            "- `S655` 与 `S652` 参数和结果完全一致，可视为重复快照。",
            "- 三组真实候选都明显依赖右尾大单：去掉前 `20` 笔最大盈利单后，`S652 / S653 / S654` 都会转负，因此它们都不是分散型稳定盈利系统。",
            "- 在尾部压力下，`S653` 比 `S652` 略稳，但优势不大；`S654` 在去掉头部大单后的塌陷最明显，说明它的“高胜率舒适感”并不等于更稳健。",
            "",
            "## 结论",
            "",
            "- 如果只在 `S652 / S653 / S654 / S655` 里选主候选，排序建议是：`S653 > S652 = S655 > S654`。",
            "- 如果你更在意心理承受，`S653` 依然是最像默认候选的一组；如果你更在意单笔弹性，才考虑 `S652`。",
            "- 这轮结论只记录研究判断，不同步任何默认参数。",
            "",
            "## 文件",
            "",
            f"- 明细表：`{REPORT_PREFIX.with_suffix('.csv')}`",
            f"- 原始摘要：`{REPORT_PREFIX.with_suffix('.json')}`",
            f"- 本说明：`{REPORT_PREFIX.with_suffix('.md')}`",
        ]
    )
    REPORT_PREFIX.with_suffix(".md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    snapshots, stresses = _build_summaries()
    _write_csv(snapshots, stresses)
    _write_json(snapshots, stresses)
    _write_markdown(snapshots, stresses)
    print(REPORT_PREFIX.with_suffix(".md"))


if __name__ == "__main__":
    main()
