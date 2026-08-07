# V11 Trade-Sequence Guardrail Report

## 1. Study Goal

Check whether short-cycle protection is worth doing by reducing risk after consecutive losses, instead of waiting for a calendar-month stop.

## 2. Fixed Trade Logic

- baseline entry gate: `v5_a_baseline`
- strong regime definition: `v5_e_close_rsi_ema55`
- cost scenario: `stress_cost_2_0x`
- exit rule: EMA21 reclaim on 4H close, execute on next 4H open
- only the risk schedule and loss-sequence throttle rule vary

## 3. Data Source

- source: `local_okx_candle_cache`
- data_root: `D:\qqokx_data`
- symbol: `BTC-USDT-SWAP`
- timeframes: `1D, 4H`

## 4. Full-History Comparison

| strategy_name                                           | start_date                | end_date                  |   initial_capital |   final_equity |   total_return |   annual_return |   max_drawdown |   profit_factor |   win_rate |   trade_count |   average_R |   median_R |   largest_win_R |   largest_loss_R |   average_holding_days |   max_consecutive_losses |    sharpe |    sortino |    calmar | schedule_name             | loss_throttle_rule_name      | cost_scenario_name   | description                                                                                                    |   fee_rate |   slippage_rate |   score |
|:--------------------------------------------------------|:--------------------------|:--------------------------|------------------:|---------------:|---------------:|----------------:|---------------:|----------------:|-----------:|--------------:|------------:|-----------:|----------------:|-----------------:|-----------------------:|-------------------------:|----------:|-----------:|----------:|:--------------------------|:-----------------------------|:---------------------|:---------------------------------------------------------------------------------------------------------------|-----------:|----------------:|--------:|
| v8_d_strong_1_5_weak_0_5__v11_c_after_4_losses_half     | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         104194 |     0.0419394  |     0.00637867  |    -0.0128674  |         1.79289 |       0.25 |            52 |     0.11222 |  -0.192329 |            4.55 |        -0.865902 |                 2.3141 |                        9 | 0.152637  | 0.0500552  | 0.495722  | v8_d_strong_1_5_weak_0_5  | v11_c_after_4_losses_half    | stress_cost_2_0x     | 1.5x strong risk, half weak risk + After 4 consecutive losses, cut risk in half until the next winning trade.  |     0.0008 |          0.0004 | 4.90494 |
| v8_d_strong_1_5_weak_0_5__v11_a_no_throttle             | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         104648 |     0.0464786  |     0.00705597  |    -0.0117346  |         1.77986 |       0.25 |            52 |     0.11222 |  -0.192329 |            4.55 |        -0.865902 |                 2.3141 |                        9 | 0.162338  | 0.0544836  | 0.601296  | v8_d_strong_1_5_weak_0_5  | v11_a_no_throttle            | stress_cost_2_0x     | 1.5x strong risk, half weak risk + No sequence guardrail. Keep normal risk sizing.                             |     0.0008 |          0.0004 | 4.88457 |
| v8_c_strong_1_25_weak_0_5__v11_a_no_throttle            | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         103849 |     0.0384866  |     0.0058618   |    -0.00994201 |         1.74243 |       0.25 |            52 |     0.11222 |  -0.192329 |            4.55 |        -0.865902 |                 2.3141 |                        9 | 0.160121  | 0.0545897  | 0.589599  | v8_c_strong_1_25_weak_0_5 | v11_a_no_throttle            | stress_cost_2_0x     | 1.25x strong risk, half weak risk + No sequence guardrail. Keep normal risk sizing.                            |     0.0008 |          0.0004 | 4.7922  |
| v8_c_strong_1_25_weak_0_5__v11_c_after_4_losses_half    | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         103409 |     0.034092   |     0.00520184  |    -0.010936   |         1.74059 |       0.25 |            52 |     0.11222 |  -0.192329 |            4.55 |        -0.865902 |                 2.3141 |                        9 | 0.148434  | 0.0488744  | 0.475662  | v8_c_strong_1_25_weak_0_5 | v11_c_after_4_losses_half    | stress_cost_2_0x     | 1.25x strong risk, half weak risk + After 4 consecutive losses, cut risk in half until the next winning trade. |     0.0008 |          0.0004 | 4.77635 |
| v8_d_strong_1_5_weak_0_5__v11_b_after_3_losses_half     | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         102606 |     0.0260618  |     0.00398978  |    -0.0116611  |         1.52961 |       0.25 |            52 |     0.11222 |  -0.192329 |            4.55 |        -0.865902 |                 2.3141 |                        9 | 0.129841  | 0.0334584  | 0.342144  | v8_d_strong_1_5_weak_0_5  | v11_b_after_3_losses_half    | stress_cost_2_0x     | 1.5x strong risk, half weak risk + After 3 consecutive losses, cut risk in half until the next winning trade.  |     0.0008 |          0.0004 | 4.23486 |
| v8_a_flat_1_0__v11_a_no_throttle                        | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         102899 |     0.0289898  |     0.00443265  |    -0.0133832  |         1.4968  |       0.25 |            52 |     0.11222 |  -0.192329 |            4.55 |        -0.865902 |                 2.3141 |                        9 | 0.134608  | 0.0390662  | 0.33121   | v8_a_flat_1_0             | v11_a_no_throttle            | stress_cost_2_0x     | Flat baseline risk on every trade + No sequence guardrail. Keep normal risk sizing.                            |     0.0008 |          0.0004 | 4.14604 |
| v8_c_strong_1_25_weak_0_5__v11_b_after_3_losses_half    | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         102093 |     0.020933   |     0.00321143  |    -0.0099292  |         1.48736 |       0.25 |            52 |     0.11222 |  -0.192329 |            4.55 |        -0.865902 |                 2.3141 |                        9 | 0.124312  | 0.0320886  | 0.323433  | v8_c_strong_1_25_weak_0_5 | v11_b_after_3_losses_half    | stress_cost_2_0x     | 1.25x strong risk, half weak risk + After 3 consecutive losses, cut risk in half until the next winning trade. |     0.0008 |          0.0004 | 4.1334  |
| v8_a_flat_1_0__v11_c_after_4_losses_half                | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         102104 |     0.0210449  |     0.00322845  |    -0.0109069  |         1.406   |       0.25 |            52 |     0.11222 |  -0.192329 |            4.55 |        -0.865902 |                 2.3141 |                        9 | 0.109     | 0.0282686  | 0.296001  | v8_a_flat_1_0             | v11_c_after_4_losses_half    | stress_cost_2_0x     | Flat baseline risk on every trade + After 4 consecutive losses, cut risk in half until the next winning trade. |     0.0008 |          0.0004 | 3.92426 |
| v8_d_strong_1_5_weak_0_5__v11_d_after_3_losses_quarter  | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         101587 |     0.0158733  |     0.00244035  |    -0.0117174  |         1.35995 |       0.25 |            52 |     0.11222 |  -0.192329 |            4.55 |        -0.865902 |                 2.3141 |                        9 | 0.0922571 | 0.0199336  | 0.208267  | v8_d_strong_1_5_weak_0_5  | v11_d_after_3_losses_quarter | stress_cost_2_0x     | 1.5x strong risk, half weak risk + After 3 consecutive losses, cut risk to 25% until the next winning trade.   |     0.0008 |          0.0004 | 3.79798 |
| v8_c_strong_1_25_weak_0_5__v11_d_after_3_losses_quarter | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         101218 |     0.0121756  |     0.00187476  |    -0.0101558  |         1.31568 |       0.25 |            52 |     0.11222 |  -0.192329 |            4.55 |        -0.865902 |                 2.3141 |                        9 | 0.0843982 | 0.0181128  | 0.184599  | v8_c_strong_1_25_weak_0_5 | v11_d_after_3_losses_quarter | stress_cost_2_0x     | 1.25x strong risk, half weak risk + After 3 consecutive losses, cut risk to 25% until the next winning trade.  |     0.0008 |          0.0004 | 3.69214 |
| v8_a_flat_1_0__v11_b_after_3_losses_half                | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         101095 |     0.0109487  |     0.0016867   |    -0.0101017  |         1.22325 |       0.25 |            52 |     0.11222 |  -0.192329 |            4.55 |        -0.865902 |                 2.3141 |                        9 | 0.0742946 | 0.0149826  | 0.166971  | v8_a_flat_1_0             | v11_b_after_3_losses_half    | stress_cost_2_0x     | Flat baseline risk on every trade + After 3 consecutive losses, cut risk in half until the next winning trade. |     0.0008 |          0.0004 | 3.45988 |
| v8_a_flat_1_0__v11_d_after_3_losses_quarter             | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         100197 |     0.00197006 |     0.000304647 |    -0.0109068  |         1.04432 |       0.25 |            52 |     0.11222 |  -0.192329 |            4.55 |        -0.865902 |                 2.3141 |                        9 | 0.0169457 | 0.00276917 | 0.0279317 | v8_a_flat_1_0             | v11_d_after_3_losses_quarter | stress_cost_2_0x     | Flat baseline risk on every trade + After 3 consecutive losses, cut risk to 25% until the next winning trade.  |     0.0008 |          0.0004 | 2.99667 |

## 5. Guardrail Usage

| strategy_name                                           | schedule_name             | loss_throttle_rule_name      |   activation_count |   reduced_risk_trade_count |   reduced_risk_win_rate |   average_effective_risk_multiplier |
|:--------------------------------------------------------|:--------------------------|:-----------------------------|-------------------:|---------------------------:|------------------------:|------------------------------------:|
| v8_a_flat_1_0__v11_a_no_throttle                        | v8_a_flat_1_0             | v11_a_no_throttle            |                  0 |                          0 |                0        |                            1        |
| v8_a_flat_1_0__v11_b_after_3_losses_half                | v8_a_flat_1_0             | v11_b_after_3_losses_half    |                  6 |                         19 |                0.263158 |                            0.817308 |
| v8_a_flat_1_0__v11_c_after_4_losses_half                | v8_a_flat_1_0             | v11_c_after_4_losses_half    |                  4 |                         13 |                0.230769 |                            0.875    |
| v8_a_flat_1_0__v11_d_after_3_losses_quarter             | v8_a_flat_1_0             | v11_d_after_3_losses_quarter |                  6 |                         19 |                0.263158 |                            0.725962 |
| v8_c_strong_1_25_weak_0_5__v11_a_no_throttle            | v8_c_strong_1_25_weak_0_5 | v11_a_no_throttle            |                  0 |                          0 |                0        |                            0.932692 |
| v8_c_strong_1_25_weak_0_5__v11_b_after_3_losses_half    | v8_c_strong_1_25_weak_0_5 | v11_b_after_3_losses_half    |                  6 |                         19 |                0.263158 |                            0.762019 |
| v8_c_strong_1_25_weak_0_5__v11_c_after_4_losses_half    | v8_c_strong_1_25_weak_0_5 | v11_c_after_4_losses_half    |                  4 |                         13 |                0.230769 |                            0.826923 |
| v8_c_strong_1_25_weak_0_5__v11_d_after_3_losses_quarter | v8_c_strong_1_25_weak_0_5 | v11_d_after_3_losses_quarter |                  6 |                         19 |                0.263158 |                            0.676683 |
| v8_d_strong_1_5_weak_0_5__v11_a_no_throttle             | v8_d_strong_1_5_weak_0_5  | v11_a_no_throttle            |                  0 |                          0 |                0        |                            1.07692  |
| v8_d_strong_1_5_weak_0_5__v11_b_after_3_losses_half     | v8_d_strong_1_5_weak_0_5  | v11_b_after_3_losses_half    |                  6 |                         19 |                0.263158 |                            0.879808 |
| v8_d_strong_1_5_weak_0_5__v11_c_after_4_losses_half     | v8_d_strong_1_5_weak_0_5  | v11_c_after_4_losses_half    |                  4 |                         13 |                0.230769 |                            0.956731 |
| v8_d_strong_1_5_weak_0_5__v11_d_after_3_losses_quarter  | v8_d_strong_1_5_weak_0_5  | v11_d_after_3_losses_quarter |                  6 |                         19 |                0.263158 |                            0.78125  |

## 6. Sequence Pressure

| strategy_name                                           |   trade_count |   max_consecutive_losses |   worst_consecutive_loss_sum |   worst_3_trade_pnl |   worst_5_trade_pnl |   worst_10_trade_pnl |
|:--------------------------------------------------------|--------------:|-------------------------:|-----------------------------:|--------------------:|--------------------:|---------------------:|
| v8_a_flat_1_0__v11_a_no_throttle                        |            52 |                        9 |                    -1369     |            -918.804 |           -1173.94  |            -1318.99  |
| v8_a_flat_1_0__v11_b_after_3_losses_half                |            52 |                        9 |                    -1026.66  |            -692.403 |            -929.505 |             -976.973 |
| v8_a_flat_1_0__v11_c_after_4_losses_half                |            52 |                        9 |                    -1109.39  |            -758.543 |           -1012.23  |            -1059.66  |
| v8_a_flat_1_0__v11_d_after_3_losses_quarter             |            52 |                        9 |                     -856.834 |            -690.155 |            -829.08  |             -971.474 |
| v8_c_strong_1_25_weak_0_5__v11_a_no_throttle            |            52 |                        9 |                    -1042.83  |            -816.868 |            -992.965 |             -905.111 |
| v8_c_strong_1_25_weak_0_5__v11_b_after_3_losses_half    |            52 |                        9 |                     -714.362 |            -714.362 |            -681.188 |             -945.239 |
| v8_c_strong_1_25_weak_0_5__v11_c_after_4_losses_half    |            52 |                        9 |                     -859.106 |            -714.677 |            -834.285 |            -1049.82  |
| v8_c_strong_1_25_weak_0_5__v11_d_after_3_losses_quarter |            52 |                        9 |                     -712.069 |            -712.069 |            -681.188 |             -965.549 |
| v8_d_strong_1_5_weak_0_5__v11_a_no_throttle             |            52 |                        9 |                    -1226.54  |            -988.834 |           -1176.29  |            -1125.22  |
| v8_d_strong_1_5_weak_0_5__v11_b_after_3_losses_half     |            52 |                        9 |                     -860.857 |            -860.857 |            -775.701 |            -1128.15  |
| v8_d_strong_1_5_weak_0_5__v11_c_after_4_losses_half     |            52 |                        9 |                    -1009.06  |            -861.362 |            -984.051 |            -1254.52  |
| v8_d_strong_1_5_weak_0_5__v11_d_after_3_losses_quarter  |            52 |                        9 |                     -857.854 |            -857.854 |            -771.113 |            -1130.05  |

## 7. Findings

- best V11 cell: v8_d_strong_1_5_weak_0_5__v11_c_after_4_losses_half with total_return=4.19%, profit_factor=1.79, max_drawdown=-1.29%
- V8-d base without sequence throttle: total_return=4.65%, profit_factor=1.78, max_drawdown=-1.17%
- V8-c base without sequence throttle: total_return=3.85%, profit_factor=1.74, max_drawdown=-0.99%
- flat base without sequence throttle: total_return=2.90%, profit_factor=1.50, max_drawdown=-1.34%
- best vs V8-d base: return_delta=-0.45%, pf_delta=0.01, drawdown_delta=-0.11%
- guardrail usage: activation_count=4, reduced_risk_trade_count=13, average_effective_risk_multiplier=0.96
- worst 10-trade pressure: candidate=-1254.52 vs V8-d base=-1125.22; max_consecutive_losses candidate=9 vs base=9

## 8. Recommendation

Carry forward: `v8_d_strong_1_5_weak_0_5__v11_c_after_4_losses_half`

## 9. Interpretation

- If a sequence throttle improves the worst loss cluster or drawdown while preserving most of the return, this research path is worth keeping.
- If it barely changes anything, then the pain is already embedded in sparse trade timing and this line should stop here.

## 10. Output Notes

- results_dir: `results_v11_trade_guardrail`
- tested_schedule_count: 3
- tested_rule_count: 4