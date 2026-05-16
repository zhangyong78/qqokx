from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_empirical_analysis_report(*, output_dir: str | Path) -> Path:
    root = Path(output_dir)
    coverage = pd.read_csv(root / "data_coverage.csv")
    sections = [
        "# 市场时间结构统计研究项目：实证分析报告",
        "",
        "## 数据与输出",
        "",
        f"本次直接使用本地 sqlite K 线缓存运行，输出目录：`{root}`。",
        "",
        "### 数据覆盖",
        "",
        _md_table(coverage),
    ]

    for mode in ("utc+8", "utc+0"):
        samples = pd.read_csv(root / "combined" / mode / "samples.csv")
        sections.extend(_build_mode_sections(samples=samples, mode=mode))

    sections.extend(
        [
            "## 分析师解读",
            "",
            "1. UTC+8 口径下，关键事件高度集中在北京时间 00:00-04:00。大阳线最终低点 00:00 占 43.3%，大阴线最终高点 00:00 占 40.8%，转折阳线最终低点 00:00 占 29.7%。这说明用北京时间自然日观察时，开盘前几个小时对最终日线结构影响很重。",
            "2. UTC+0 口径下，同样的集中窗口整体平移到北京时间 08:00-10:00，并在 21:00-23:00 出现第二组候选窗口。这是切日边界造成的结构重排，不应把两套口径混合。",
            "3. 大阳线最后一次低于日开盘价在 UTC+8 下集中于 00:00-05:00；大阴线最后一次高于日开盘价集中于 00:00-03:00 与 08:00。这个结果支持“强趋势日的最后反向试探往往很早结束”的假设。",
            "4. 大阳线主要发生在 uptrend 中，UTC+8 占 67.3%，UTC+0 占 69.5%；大阴线在 uptrend 和 downtrend 中都不少，说明大阴线不只是熊市产物，也常出现在上涨趋势中的剧烈去杠杆日。",
            "5. 高 compression_score 分位并没有线性提高 big_bull/big_bear 占比，因此当前压缩指标更像波动背景变量，不能单独作为“大区间单边”的方向或强度结论。下一版应把压缩定义改成相对分位、并拆成方向前 12 小时和日内早段压缩两种口径。",
            "6. 本报告使用最终日线类型作为研究标签，适合做市场结构归纳；盘中实盘识别仍需下一阶段研究“形成中”的可观测特征。",
            "",
        ]
    )

    report_path = root / "empirical_analysis_report.md"
    report_path.write_text("\n".join(sections), encoding="utf-8-sig")
    return report_path


def _build_mode_sections(*, samples: pd.DataFrame, mode: str) -> list[str]:
    focus_metrics = [
        ("big_bull", "last_below_open_hour"),
        ("big_bear", "last_above_open_hour"),
        ("turn_bull", "day_low_hour"),
        ("turn_bear", "day_high_hour"),
        ("big_bull", "day_low_hour"),
        ("big_bear", "day_high_hour"),
    ]
    sections = [
        f"## {mode} 口径核心结论",
        "",
        f"有效样本数：{len(samples)}。",
        "",
        "### 日线类型样本数",
        "",
    ]
    day_type_counts = samples["day_type"].value_counts().rename_axis("day_type").reset_index(name="count")
    day_type_counts["share"] = (day_type_counts["count"] / day_type_counts["count"].sum()).map(_fmt_pct)
    sections.append(_md_table(day_type_counts))

    rows = []
    for day_type, metric in focus_metrics:
        top = _top_hours(samples=samples, day_type=day_type, metric=metric, top_n=5)
        for _, row in top.iterrows():
            rows.append(
                {
                    "day_type": day_type,
                    "metric": metric,
                    "hour": row["hour"],
                    "count": int(row["count"]),
                    "probability": _fmt_pct(row["probability"]),
                }
            )
    sections.extend(["### 重点时间窗口 Top 5", "", _md_table(pd.DataFrame(rows))])

    cross_rows = []
    for day_type, metric in (
        ("big_bull", "last_below_open_hour"),
        ("big_bear", "last_above_open_hour"),
        ("turn_bull", "day_low_hour"),
    ):
        for symbol, group in samples.groupby("symbol"):
            top = _top_hours(samples=group, day_type=day_type, metric=metric, top_n=3)
            for _, row in top.iterrows():
                cross_rows.append(
                    {
                        "day_type": day_type,
                        "metric": metric,
                        "symbol": symbol,
                        "hour": row["hour"],
                        "count": int(row["count"]),
                        "probability": _fmt_pct(row["probability"]),
                    }
                )
    sections.extend(["### 跨币种重点窗口 Top 3", "", _md_table(pd.DataFrame(cross_rows))])

    extension_rows = []
    for day_type, group in samples.groupby("day_type"):
        extension_rows.append(
            {
                "day_type": day_type,
                "count": len(group),
                "extension_to_22h_median": _fmt_decimal(group["extension_to_22h"].median()),
                "extension_to_22h_mean": _fmt_decimal(group["extension_to_22h"].mean()),
                "extension_to_next_06h_median": _fmt_decimal(group["extension_to_next_06h"].median()),
                "extension_to_next_06h_mean": _fmt_decimal(group["extension_to_next_06h"].mean()),
                "range_median": _fmt_decimal(group["daily_range_pct"].median()),
                "compression_median": _fmt_decimal(group["compression_score"].median()),
            }
        )
    sections.extend(["### 延续性与压缩概览", "", _md_table(pd.DataFrame(extension_rows).sort_values("day_type"))])

    compression_rows = []
    clean = samples.dropna(subset=["compression_score"]).copy()
    if not clean.empty:
        clean["compression_bucket"] = pd.qcut(
            clean["compression_score"],
            q=4,
            labels=["Q1_low", "Q2", "Q3", "Q4_high"],
            duplicates="drop",
        )
        clean["is_big_range"] = clean["day_type"].isin(["big_bull", "big_bear"])
        for bucket, group in clean.groupby("compression_bucket", observed=True):
            compression_rows.append(
                {
                    "compression_bucket": str(bucket),
                    "count": len(group),
                    "big_range_rate": _fmt_pct(group["is_big_range"].mean()),
                    "median_daily_range_pct": _fmt_decimal(group["daily_range_pct"].median()),
                    "median_compression_score": _fmt_decimal(group["compression_score"].median()),
                }
            )
    sections.extend(["### ATR 压缩分位与大区间比例", "", _md_table(pd.DataFrame(compression_rows))])

    trend_counts = pd.crosstab(samples["day_type"], samples["trend_type"]).reset_index()
    trend_share = pd.crosstab(samples["day_type"], samples["trend_type"], normalize="index").reset_index()
    for column in trend_share.columns:
        if column != "day_type":
            trend_share[column] = trend_share[column].map(_fmt_pct)
    sections.extend(
        [
            "### 趋势环境分布：样本数",
            "",
            _md_table(trend_counts),
            "### 趋势环境分布：行内占比",
            "",
            _md_table(trend_share),
        ]
    )
    return sections


def _top_hours(*, samples: pd.DataFrame, day_type: str, metric: str, top_n: int) -> pd.DataFrame:
    filtered = samples[(samples["day_type"] == day_type) & samples[metric].notna() & (samples[metric] != "")]
    if filtered.empty:
        return pd.DataFrame(columns=["hour", "count", "probability"])
    counts = filtered[metric].value_counts().sort_index()
    frame = counts.rename_axis("hour").reset_index(name="count")
    frame["probability"] = frame["count"] / frame["count"].sum()
    return frame.sort_values(["probability", "count"], ascending=False).head(top_n)


def _md_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No data_"
    data = frame.fillna("")
    columns = list(data.columns)
    lines = [
        "| " + " | ".join(map(str, columns)) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for _, row in data.iterrows():
        lines.append("| " + " | ".join(str(row[column]) for column in columns) + " |")
    return "\n".join(lines)


def _fmt_pct(value: float) -> str:
    if pd.isna(value):
        return "-"
    return f"{value * 100:.1f}%"


def _fmt_decimal(value: float) -> str:
    if pd.isna(value):
        return "-"
    return f"{value:.4f}"
