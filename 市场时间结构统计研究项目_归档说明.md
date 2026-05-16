# 市场时间结构统计研究项目归档说明

## 项目定位

本项目是一个市场时间结构统计研究项目，不是交易策略收益优化项目。

研究目标是基于 BTC、ETH、SOL 的历史 1H 与 1D K 线，观察当日线最终形成特定结构时，其日内 1 小时路径是否存在稳定的时间规律。

核心问题是：

- 转折阳线的最终低点，通常在哪个小时形成。
- 大阳线形成前，最后一次低于日开盘价通常几点结束。
- 大阴线形成前，最后一次高于日开盘价通常几点结束。
- 22 点后与次日 06 点前是否存在延续惯性。
- ATR 压缩后是否更容易出现大区间单边。
- 不同 EMA200 趋势环境下，上述规律是否发生变化。

## 数据口径

输入数据字段：

- timestamp
- open
- high
- low
- close
- volume

时间统一使用 UTC 存储。程序分别支持 UTC+8 切日和 UTC+0 切日，两套口径需要分开分析，不建议混合。

## 日线分类

研究样本包含六类：

- turn_bull：转折阳线
- mid_bull：中阳线
- big_bull：大阳线
- turn_bear：转折阴线
- mid_bear：中阴线
- big_bear：大阴线

大阳线条件：

- close > open
- 实体长度 / 全日振幅 >= 0.6
- 收盘距离最高点 <= 全日振幅 20%
- 全日振幅 > ATR20 * 1.2

中阳线条件：

- close > open
- 实体长度 / 全日振幅在 0.35 到 0.6 之间

转折阳线条件：

- 前 1 到 3 日存在弱势
- 当天 close > open
- 满足 close 突破前一日中轴、实体吞没前一日实体、长下影反转之一

阴线规则按镜像处理。

## 输出文件

每次运行会导出以下文件：

- samples.csv
- summary.csv
- heatmap_source.csv
- research_brief.md

### samples.csv

每个有效交易日一行，是最重要的明细表。

字段包括：

- date
- symbol
- close_mode
- day_type
- trend_type
- daily_range_pct
- body_ratio
- upper_shadow_ratio
- lower_shadow_ratio
- day_low_hour
- day_high_hour
- last_below_open_hour
- last_above_open_hour
- extension_to_22h
- extension_to_next_06h
- atr20
- compression_score

### summary.csv

按 day_type、trend_type、metric、hour 统计样本数量与概率。

metric 包含：

- day_low_hour
- day_high_hour
- last_below_open_hour
- last_above_open_hour

适合快速观察某类日线在不同趋势环境下，关键小时是否集中。

### heatmap_source.csv

用于绘制小时概率热力图。重点建议先画：

- turn_bull + day_low_hour
- big_bull + day_low_hour
- big_bear + day_high_hour
- big_bull + last_below_open_hour
- big_bear + last_above_open_hour

## 分析重点

建议分析师优先回答这些问题：

- 转折阳线最终低点是否集中在固定时间窗口。
- 大阳线最后一次低于日开盘价，是否通常在某些小时结束。
- 大阴线最后一次高于日开盘价，是否通常在某些小时结束。
- day_low_hour 与 day_high_hour 是否在 UTC+8 和 UTC+0 两套口径下保持稳定。
- uptrend、downtrend、sideways 中，同类 day_type 的时间分布是否明显不同。
- 高 compression_score 是否对应更高比例的 big_bull 或 big_bear。
- 22 点后延续和次日 06 点前延续，是否能区分真单边与假突破。

## 防偏差要求

本研究避免以下问题：

- lookahead bias
- 数据污染
- 时间错位
- UTC 切日错误
- 将交易收益优化误当作结构统计结论

日线分类虽然以收盘后最终形态为样本标签，但日内字段只统计该交易日 1H K 线本身的客观结构，不引入未来交易日信息。

## 运行示例

同时输出 UTC+8 与 UTC+0 两套结果：

```bash
python scripts/run_daily_turn_stats.py --hourly data/btc_1h.csv --daily data/btc_1d.csv --output-dir dist/daily_turn_stats/btc --symbol BTC-USDT-SWAP --close-mode both
```

输出目录示例：

```text
dist/daily_turn_stats/btc/
  utc+8/
    samples.csv
    summary.csv
    heatmap_source.csv
    research_brief.md
  utc+0/
    samples.csv
    summary.csv
    heatmap_source.csv
    research_brief.md
```

## 后续研究方向

- 时间窗口过滤
- 多周期共振
- 日内结构识别
- AI 结构分类
- 自动化交易系统
