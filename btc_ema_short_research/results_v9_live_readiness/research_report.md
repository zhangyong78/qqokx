# V9 Live Readiness Report

## 1. Study Goal

Carry the V8 winner into a more practical live-readiness pass by checking month-by-month, quarter-by-quarter, and loss-streak pressure.

## 2. Fixed Trade Logic

- baseline entry gate: `v5_a_baseline`
- strong regime definition: `v5_e_close_rsi_ema55`
- exit rule: EMA21 reclaim on 4H close, execute on next 4H open
- sizing schedules and cost assumptions come from the V8 shortlist only

## 3. Data Source

- source: `local_okx_candle_cache`
- data_root: `D:\qqokx_data`
- symbol: `BTC-USDT-SWAP`
- timeframes: `1D, 4H`

## 4. Full-History Shortlist

| strategy_name                               | start_date                | end_date                  |   initial_capital |   final_equity |   total_return |   annual_return |   max_drawdown |   profit_factor |   win_rate |   trade_count |   average_R |   median_R |   largest_win_R |   largest_loss_R |   average_holding_days |   max_consecutive_losses |   sharpe |   sortino |   calmar | schedule_name             | cost_scenario_name   | description                                          |   fee_rate |   slippage_rate |   score |
|:--------------------------------------------|:--------------------------|:--------------------------|------------------:|---------------:|---------------:|----------------:|---------------:|----------------:|-----------:|--------------:|------------:|-----------:|----------------:|-----------------:|-----------------------:|-------------------------:|---------:|----------:|---------:|:--------------------------|:---------------------|:-----------------------------------------------------|-----------:|----------------:|--------:|
| v8_d_strong_1_5_weak_0_5__base_cost         | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         105320 |      0.053196  |      0.00805373 |    -0.0108311  |         1.95508 |   0.288462 |            52 |    0.134734 |  -0.172506 |         4.59734 |        -0.848008 |                 2.3141 |                        9 | 0.182885 | 0.0644701 | 0.743576 | v8_d_strong_1_5_weak_0_5  | base_cost            | Best dynamic sizing under original costs             |     0.0004 |          0.0002 | 5.42608 |
| v8_d_strong_1_5_weak_0_5__stress_cost_2_0x  | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         104648 |      0.0464786 |      0.00705597 |    -0.0117346  |         1.77986 |   0.25     |            52 |    0.11222  |  -0.192329 |         4.55    |        -0.865902 |                 2.3141 |                        9 | 0.162338 | 0.0544836 | 0.601296 | v8_d_strong_1_5_weak_0_5  | stress_cost_2_0x     | Best dynamic sizing under harsh cost assumptions     |     0.0008 |          0.0004 | 4.88457 |
| v8_c_strong_1_25_weak_0_5__stress_cost_2_0x | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         103849 |      0.0384866 |      0.0058618  |    -0.00994201 |         1.74243 |   0.25     |            52 |    0.11222  |  -0.192329 |         4.55    |        -0.865902 |                 2.3141 |                        9 | 0.160121 | 0.0545897 | 0.589599 | v8_c_strong_1_25_weak_0_5 | stress_cost_2_0x     | Moderate dynamic sizing under harsh cost assumptions |     0.0008 |          0.0004 | 4.7922  |
| v8_a_flat_1_0__base_cost                    | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         103502 |      0.0350207 |      0.00534152 |    -0.0126367  |         1.64119 |   0.288462 |            52 |    0.134734 |  -0.172506 |         4.59734 |        -0.848008 |                 2.3141 |                        9 | 0.160301 | 0.0484633 | 0.422697 | v8_a_flat_1_0             | base_cost            | Flat baseline under original cost assumptions        |     0.0004 |          0.0002 | 4.60881 |
| v8_a_flat_1_0__stress_cost_2_0x             | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         102899 |      0.0289898 |      0.00443265 |    -0.0133832  |         1.4968  |   0.25     |            52 |    0.11222  |  -0.192329 |         4.55    |        -0.865902 |                 2.3141 |                        9 | 0.134608 | 0.0390662 | 0.33121  | v8_a_flat_1_0             | stress_cost_2_0x     | Flat baseline under harsh cost assumptions           |     0.0008 |          0.0004 | 4.14604 |

## 5. Monthly Stability Summary

| strategy_name                               | schedule_name             | cost_scenario_name   | period_freq   |   period_count |   positive_periods |   positive_period_rate |   median_period_return |   worst_period_return |   worst_period_drawdown |   median_profit_factor |   average_trade_count |
|:--------------------------------------------|:--------------------------|:---------------------|:--------------|---------------:|-------------------:|-----------------------:|-----------------------:|----------------------:|------------------------:|-----------------------:|----------------------:|
| v8_d_strong_1_5_weak_0_5__base_cost         | v8_d_strong_1_5_weak_0_5  | base_cost            | M             |             27 |                  9 |               0.333333 |           -0.000362482 |           -0.00879142 |             -0.00879142 |                      0 |               1.92593 |
| v8_c_strong_1_25_weak_0_5__stress_cost_2_0x | v8_c_strong_1_25_weak_0_5 | stress_cost_2_0x     | M             |             27 |                  9 |               0.333333 |           -0.000479954 |           -0.00780084 |             -0.00780084 |                      0 |               1.92593 |
| v8_d_strong_1_5_weak_0_5__stress_cost_2_0x  | v8_d_strong_1_5_weak_0_5  | stress_cost_2_0x     | M             |             27 |                  9 |               0.333333 |           -0.000517531 |           -0.00935626 |             -0.00935626 |                      0 |               1.92593 |
| v8_a_flat_1_0__base_cost                    | v8_a_flat_1_0             | base_cost            | M             |             27 |                 10 |               0.37037  |           -0.00067556  |           -0.00683127 |             -0.00683127 |                      0 |               1.92593 |
| v8_a_flat_1_0__stress_cost_2_0x             | v8_a_flat_1_0             | stress_cost_2_0x     | M             |             27 |                 10 |               0.37037  |           -0.000807363 |           -0.00704806 |             -0.00704806 |                      0 |               1.92593 |

## 6. Quarterly Stability Summary

| strategy_name                               | schedule_name             | cost_scenario_name   | period_freq   |   period_count |   positive_periods |   positive_period_rate |   median_period_return |   worst_period_return |   worst_period_drawdown |   median_profit_factor |   average_trade_count |
|:--------------------------------------------|:--------------------------|:---------------------|:--------------|---------------:|-------------------:|-----------------------:|-----------------------:|----------------------:|------------------------:|-----------------------:|----------------------:|
| v8_d_strong_1_5_weak_0_5__base_cost         | v8_d_strong_1_5_weak_0_5  | base_cost            | Q             |             17 |                  6 |               0.352941 |           -0.000430014 |           -0.00497506 |             -0.0104055  |                      0 |               3.05882 |
| v8_a_flat_1_0__base_cost                    | v8_a_flat_1_0             | base_cost            | Q             |             17 |                  7 |               0.411765 |           -0.000469655 |           -0.00683127 |             -0.00852188 |                      0 |               3.05882 |
| v8_c_strong_1_25_weak_0_5__stress_cost_2_0x | v8_c_strong_1_25_weak_0_5 | stress_cost_2_0x     | Q             |             17 |                  6 |               0.352941 |           -0.000479954 |           -0.00474318 |             -0.00946659 |                      0 |               3.05882 |
| v8_d_strong_1_5_weak_0_5__stress_cost_2_0x  | v8_d_strong_1_5_weak_0_5  | stress_cost_2_0x     | Q             |             17 |                  6 |               0.352941 |           -0.000479954 |           -0.00514843 |             -0.0111103  |                      0 |               3.05882 |
| v8_a_flat_1_0__stress_cost_2_0x             | v8_a_flat_1_0             | stress_cost_2_0x     | Q             |             17 |                  6 |               0.352941 |           -0.000650794 |           -0.00704806 |             -0.00903315 |                      0 |               3.05882 |

## 7. Loss-Streak Pressure

| strategy_name                               |   trade_count |   max_consecutive_losses |   worst_consecutive_loss_sum |   worst_3_trade_pnl |   worst_5_trade_pnl |   worst_10_trade_pnl | schedule_name             | cost_scenario_name   | description                                          |
|:--------------------------------------------|--------------:|-------------------------:|-----------------------------:|--------------------:|--------------------:|---------------------:|:--------------------------|:---------------------|:-----------------------------------------------------|
| v8_a_flat_1_0__base_cost                    |            52 |                        9 |                     -1297.54 |            -878.86  |           -1118.76  |            -1229.48  | v8_a_flat_1_0             | base_cost            | Flat baseline under original cost assumptions        |
| v8_a_flat_1_0__stress_cost_2_0x             |            52 |                        9 |                     -1369    |            -918.804 |           -1173.94  |            -1318.99  | v8_a_flat_1_0             | stress_cost_2_0x     | Flat baseline under harsh cost assumptions           |
| v8_c_strong_1_25_weak_0_5__stress_cost_2_0x |            52 |                        9 |                     -1042.83 |            -816.868 |            -992.965 |             -905.111 | v8_c_strong_1_25_weak_0_5 | stress_cost_2_0x     | Moderate dynamic sizing under harsh cost assumptions |
| v8_d_strong_1_5_weak_0_5__base_cost         |            52 |                        9 |                     -1153.22 |            -934.523 |           -1107.91  |            -1024.77  | v8_d_strong_1_5_weak_0_5  | base_cost            | Best dynamic sizing under original costs             |
| v8_d_strong_1_5_weak_0_5__stress_cost_2_0x  |            52 |                        9 |                     -1226.54 |            -988.834 |           -1176.29  |            -1125.22  | v8_d_strong_1_5_weak_0_5  | stress_cost_2_0x     | Best dynamic sizing under harsh cost assumptions     |

## 8. Findings

- best original-cost cell: v8_d_strong_1_5_weak_0_5__base_cost with total_return=5.32%, profit_factor=1.96, max_drawdown=-1.08%
- harsh-cost baseline: v8_a_flat_1_0__stress_cost_2_0x with total_return=2.90%, profit_factor=1.50, max_drawdown=-1.34%
- harsh-cost dynamic candidate: v8_d_strong_1_5_weak_0_5__stress_cost_2_0x with total_return=4.65%, profit_factor=1.78, max_drawdown=-1.17%
- harsh-cost return delta vs flat baseline: 1.75%
- harsh-cost profit-factor delta vs flat baseline: 0.28
- harsh-cost drawdown delta vs flat baseline: 0.16%
- harsh-cost worst month: candidate=-0.94% vs baseline=-0.70%; positive month rate: candidate=33.33% vs baseline=37.04%
- harsh-cost worst quarter: candidate=-0.51% vs baseline=-0.70%; positive quarter rate: candidate=35.29% vs baseline=35.29%
- harsh-cost loss streak pressure: candidate max_consecutive_losses=9, baseline=9; worst_10_trade_pnl candidate=-1125.22, baseline=-1318.99

## 9. Recommendation

Carry forward: `v8_d_strong_1_5_weak_0_5__stress_cost_2_0x`

## 10. Interpretation

- V9 is not trying to find a brand-new signal. It is checking whether the current favorite still looks acceptable when inspected through practical pain points that traders actually experience.
- If a candidate survives harsher cost assumptions while also keeping monthly and quarterly damage controlled, it is a much better live candidate than a version that only looks good in one aggregate equity curve.

## 11. Output Notes

- results_dir: `results_v9_live_readiness`
- shortlist_count: 5
- month_count_range: 27-27
- quarter_count_range: 17-17
- base_cost_baseline_reference: `v8_a_flat_1_0__base_cost`