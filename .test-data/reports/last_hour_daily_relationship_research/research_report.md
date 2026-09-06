# 7:00-8:00 最后一小时 vs 下一根 8:00 日线 研究报告

## 研究设置
- 标的：`BTC-USDT-SWAP`（默认缓存标识 `BTC-USDT-SWAP`，周期 `1H`）
- 数据来源：本地 candle cache（`BTC-USDT-SWAP` / `1H`）
- 日线定义：本地时间每天 `08:00` 到次日 `08:00` 聚合
- 信号 K 线：本地时间每天 `07:00-08:00` 的 1 小时 K 线
- 入场假设：信号 K 线收盘后立即入场
- 多头止损：信号 K 线低点；空头止损：信号 K 线高点
- 收益统计：下一根 8:00 日线内是否触发止损、是否触发 1R/2R、下一日线收盘 R、止损约束后的期望 R
- 同根小时线内同时触及止损和目标时，按保守口径记为 `stop first`，避免乐观偏差
- 过滤说明：报告中的重点条件默认要求样本数不少于 `30`，以降低小样本误判

## 数据范围
- 样本数：`2373`
- 信号起点：`2019-12-17`
- 信号终点：`2026-06-15`
- 时区偏移：`UTC+8`

## 全样本基线

### Long（按最后一小时收盘做多）
- 样本数：`2373`
- 下一日日线收阳率：`51.2%`；收阴率：`48.8%`
- 1R 命中率：`81.4%`；2R 命中率：`71.8%`；止损率：`88.7%`
- 下一日线收盘平均 R：`-14.74`；止损约束后的期望 R：`0.32`

### Short（按最后一小时收盘做空）
- 样本数：`2373`
- 下一日日线收阳率：`51.2%`；收阴率：`48.8%`
- 1R 命中率：`81.7%`；2R 命中率：`72.6%`；止损率：`88.8%`
- 下一日线收盘平均 R：`4.33`；止损约束后的期望 R：`-0.01`

## 第一版重点条件
- 已纳入的条件：前一日日线阴阳、最后一小时阴阳、最后一小时强弱、影线、量能、趋势背景、日内最后一小时突破、前一日相对前日突破。
- 完整结果见 `condition_summary.csv`，以下只摘录样本数足够的高信号条件。

### Long Top Conditions
| condition_group | condition_value | sample_count | expectancy_r | avg_final_close_r | hit_1r_rate | hit_2r_rate | stop_rate |
| --- | --- | --- | --- | --- | --- | --- | --- |
| trend_signal_combo | sideways__bear | 360 | 1.21 | 35.72 | 79.7% | 76.0% | 93.8% |
| breakout_signal_combo | close_break_session_low__bear | 78 | 0.94 | -9.17 | 80.8% | 70.5% | 91.0% |
| signal_breakout_bucket | close_break_session_low | 78 | 0.94 | -9.17 | 80.8% | 70.5% | 91.0% |
| prev_day_breakout_bucket | inside_prior_day_range | 498 | 0.86 | -74.92 | 83.2% | 75.1% | 89.2% |
| breakout_signal_combo | wick_break_session_high__bear | 38 | 0.77 | -6.71 | 78.9% | 78.9% | 84.2% |
| prev_day_signal_combo | bull__bear | 515 | 0.66 | 32.79 | 78.3% | 73.4% | 93.5% |
| prev_day_trend_bucket | sideways | 735 | 0.64 | 17.56 | 78.3% | 68.7% | 88.5% |
| signal_color | bear | 1187 | 0.53 | -30.46 | 83.2% | 78.1% | 92.9% |

### Short Top Conditions
| condition_group | condition_value | sample_count | expectancy_r | avg_final_close_r | hit_1r_rate | hit_2r_rate | stop_rate |
| --- | --- | --- | --- | --- | --- | --- | --- |
| breakout_signal_combo | wick_break_session_low__bear | 48 | 0.63 | -1.52 | 62.5% | 54.2% | 85.4% |
| prev_day_breakout_bucket | wick_break_prior_low | 360 | 0.57 | 4.15 | 80.9% | 73.0% | 89.9% |
| signal_wick_bucket | two_sided | 157 | 0.52 | 1.48 | 88.5% | 82.2% | 90.4% |
| breakout_signal_combo | close_break_session_low__bear | 78 | 0.33 | 0.22 | 64.1% | 44.9% | 61.5% |
| signal_breakout_bucket | close_break_session_low | 78 | 0.33 | 0.22 | 64.1% | 44.9% | 61.5% |
| signal_volume_bucket | high_volume | 270 | 0.33 | -0.61 | 74.8% | 60.7% | 75.2% |
| signal_strength_bucket | weak | 900 | 0.28 | -24.88 | 85.1% | 77.1% | 89.8% |
| prev_day_signal_combo | bull__bear | 515 | 0.22 | 0.54 | 83.5% | 71.3% | 82.1% |

### Long Avoid Conditions
| condition_group | condition_value | sample_count | expectancy_r | avg_final_close_r | hit_1r_rate | hit_2r_rate | stop_rate |
| --- | --- | --- | --- | --- | --- | --- | --- |
| prev_day_breakout_bucket | close_break_prior_low | 457 | -0.19 | 0.46 | 84.6% | 73.8% | 90.5% |
| trend_signal_combo | downtrend__bull | 326 | -0.10 | 0.81 | 83.1% | 68.4% | 87.1% |
| breakout_signal_combo | wick_break_session_high__bull | 53 | -0.08 | -0.48 | 79.2% | 66.0% | 90.6% |
| prev_day_trend_bucket | downtrend | 698 | -0.01 | -39.24 | 84.2% | 74.1% | 90.3% |
| breakout_signal_combo | close_break_session_high__bull | 81 | -0.01 | 0.25 | 69.1% | 43.2% | 67.9% |
| signal_breakout_bucket | close_break_session_high | 81 | -0.01 | 0.25 | 69.1% | 43.2% | 67.9% |

### Short Avoid Conditions
| condition_group | condition_value | sample_count | expectancy_r | avg_final_close_r | hit_1r_rate | hit_2r_rate | stop_rate |
| --- | --- | --- | --- | --- | --- | --- | --- |
| breakout_signal_combo | wick_break_session_high__bear | 38 | -0.39 | -0.11 | 63.2% | 44.7% | 73.7% |
| prev_day_breakout_bucket | wick_break_prior_high | 497 | -0.27 | -55.54 | 81.9% | 70.5% | 88.0% |
| signal_breakout_bucket | wick_break_session_high | 91 | -0.26 | -0.05 | 73.6% | 61.5% | 83.5% |
| prev_day_breakout_bucket | inside_prior_day_range | 498 | -0.24 | 49.28 | 82.8% | 74.7% | 91.5% |
| trend_signal_combo | uptrend__bull | 484 | -0.23 | 5.73 | 83.1% | 79.3% | 93.7% |
| prev_day_signal_combo | bull__bull | 695 | -0.22 | -4.39 | 85.5% | 81.1% | 93.1% |

## 结果解读建议
- `avg_final_close_r` 反映“如果完全持有到下一根 8:00 日线收盘”的方向性优势。
- 当止损非常贴近入场时，`avg_final_close_r` 可能被少量极端样本放大，因此判断优先级时应更看重 `expectancy_r`、`stop_rate` 和样本数。
- `expectancy_r` 反映“带止损但不设止盈、持有到下一根 8:00 收盘”的更接近交易执行的预期值。
- `hit_1r_rate` / `hit_2r_rate` 可以帮助判断是否值得在第二版研究里加入分批止盈、保本或移动止损规则。
- 如果某个条件的命中率看起来很好，但样本数太低，应优先怀疑偶然性而不是直接上实盘结论。
