# V2 Research Report

## 1. Study Goal

Use daily bars to define BTC short direction, then use 4H bars to improve entry timing and compare which 4H trigger is most practical.

## 2. Data Source

- source: `local_okx_candle_cache`
- data_root: `D:\qqokx_data`
- symbol: `BTC-USDT-SWAP`
- daily_timeframe: `1D`
- entry_timeframe: `4H`

## 3. Data Range

- daily first bar: 2019-11-27T16:00:00+00:00
- daily last bar: 2026-06-01T16:00:00+00:00
- daily bar count: 2379
- 4H first bar: 2019-12-16T04:00:00+00:00
- 4H last bar: 2026-06-02T12:00:00+00:00
- 4H bar count: 14163

## 4. V2 Design

- daily_filter_core = daily EMA21 < EMA55 and daily close < EMA55
- daily_filter_volume = daily_filter_core plus daily volume >= daily VOL_MA20
- daily_filter_ema55 = daily close < EMA55 and daily EMA55 slope over 5 bars < 0
- 4H entries still trigger on bar close and execute on next 4H open
- stop and fees keep the original project rules

## 5. Strategy Summary

| strategy_name                         | start_date                | end_date                  |   initial_capital |   final_equity |   total_return |   annual_return |   max_drawdown |   profit_factor |   win_rate |   trade_count |   average_R |   median_R |   largest_win_R |   largest_loss_R |   average_holding_days |   max_consecutive_losses |     sharpe |    sortino |     calmar |    score |
|:--------------------------------------|:--------------------------|:--------------------------|------------------:|---------------:|---------------:|----------------:|---------------:|----------------:|-----------:|--------------:|------------:|-----------:|----------------:|-----------------:|-----------------------:|-------------------------:|-----------:|-----------:|-----------:|---------:|
| v2_e_daily_volume_4h_dual_bear_volume | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |       103502   |      0.0350207 |      0.00534152 |     -0.0126367 |        1.64119  |   0.288462 |            52 |   0.134734  |  -0.172506 |         4.59734 |        -0.848008 |                2.3141  |                        9 |  0.160301  |  0.0484633 |  0.422697  |  4.60881 |
| v2_d_daily_core_4h_dual_bear_volume   | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |       101437   |      0.0143715 |      0.00221084 |     -0.0233666 |        1.09874  |   0.243902 |           123 |   0.0252122 |  -0.204043 |         4.9932  |        -1.02715  |                1.80488 |                       12 |  0.0497942 |  0.0193035 |  0.0946154 |  2.7252  |
| v2_a_daily_core_4h_ema21_pullback     | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |        94726.8 |     -0.0527325 |     -0.00834925 |     -0.0703526 |        0.790346 |   0.240343 |           233 |  -0.0452754 |  -0.206922 |         4.9932  |        -1.06153  |                1.67954 |                       12 | -0.157734  | -0.0707497 | -0.118677  | -1.69415 |
| v2_f_daily_ema55_4h_ema55_pullback    | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |        97210   |     -0.0278999 |     -0.00436978 |     -0.0423302 |        0.632178 |   0.25     |            72 |  -0.0780369 |  -0.168872 |         1.90023 |        -1.01453  |                1.45833 |                       10 | -0.223575  | -0.0341675 | -0.103231  | -2.02064 |
| v2_b_daily_core_4h_ema55_pullback     | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |        96149.6 |     -0.0385044 |     -0.00605854 |     -0.0448554 |        0.480039 |   0.230769 |            65 |  -0.120332  |  -0.207226 |         1.19894 |        -1.01453  |                1.43846 |                       10 | -0.353822  | -0.0467337 | -0.135068  | -2.59883 |
| v2_c_daily_core_4h_dual_bear_rsi      | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |        94095.3 |     -0.0590471 |     -0.00937523 |     -0.0672135 |        0.448966 |   0.228814 |           118 |  -0.102803  |  -0.144468 |         1.53659 |        -1.06153  |                1.01412 |                       13 | -0.477247  | -0.0850148 | -0.139484  | -2.76708 |

## 6. Yearly Performance

|   year | strategy_name                         |   year_start_equity |   year_end_equity |   year_return |   trade_count |   win_rate |   max_drawdown |   profit_factor |
|-------:|:--------------------------------------|--------------------:|------------------:|--------------:|--------------:|-----------:|---------------:|----------------:|
|   2020 | v2_a_daily_core_4h_ema21_pullback     |            100000   |           99143.9 |  -0.00856133  |            12 |   0.416667 |    -0.00861792 |        0.340085 |
|   2021 | v2_a_daily_core_4h_ema21_pullback     |             99143.9 |           98292.2 |  -0.00859029  |            27 |   0.37037  |    -0.0110178  |        0.642893 |
|   2022 | v2_a_daily_core_4h_ema21_pullback     |             98292.2 |           95689.8 |  -0.0264762   |            85 |   0.211765 |    -0.0340607  |        0.742852 |
|   2023 | v2_a_daily_core_4h_ema21_pullback     |             95689.8 |           94435   |  -0.0131129   |            21 |   0.238095 |    -0.0178685  |        0.505286 |
|   2024 | v2_a_daily_core_4h_ema21_pullback     |             94435   |           94210.4 |  -0.00237904  |            16 |   0.25     |    -0.00996847 |        0.822556 |
|   2025 | v2_a_daily_core_4h_ema21_pullback     |             94210.4 |           93343.9 |  -0.00919715  |            51 |   0.156863 |    -0.0289357  |        0.847554 |
|   2026 | v2_a_daily_core_4h_ema21_pullback     |             93343.9 |           94726.8 |   0.0148147   |            21 |   0.285714 |    -0.00731821 |        1.74212  |
|   2020 | v2_b_daily_core_4h_ema55_pullback     |            100000   |           99270.1 |  -0.00729873  |             2 |   0        |    -0.00507266 |        0        |
|   2021 | v2_b_daily_core_4h_ema55_pullback     |             99270.1 |           99622.6 |   0.00355047  |             8 |   0.625    |    -0.00325148 |        2.08754  |
|   2022 | v2_b_daily_core_4h_ema55_pullback     |             99622.6 |           98310.1 |  -0.0131749   |            25 |   0.24     |    -0.0183835  |        0.494912 |
|   2023 | v2_b_daily_core_4h_ema55_pullback     |             98310.1 |           97952   |  -0.0036417   |             4 |   0.25     |    -0.00507381 |        0.418174 |
|   2024 | v2_b_daily_core_4h_ema55_pullback     |             97952   |           97471.4 |  -0.00490686  |             4 |   0        |    -0.00474522 |        0        |
|   2025 | v2_b_daily_core_4h_ema55_pullback     |             97471.4 |           96007.2 |  -0.0150221   |            17 |   0.117647 |    -0.0179855  |        0.342118 |
|   2026 | v2_b_daily_core_4h_ema55_pullback     |             96007.2 |           96149.6 |   0.00148293  |             5 |   0.2      |    -0.00203979 |        1.33027  |
|   2020 | v2_c_daily_core_4h_dual_bear_rsi      |            100000   |           99775.1 |  -0.00224912  |             2 |   0        |    -0.00102022 |        0        |
|   2021 | v2_c_daily_core_4h_dual_bear_rsi      |             99775.1 |          100223   |   0.00448837  |            13 |   0.538462 |    -0.00201332 |        1.75002  |
|   2022 | v2_c_daily_core_4h_dual_bear_rsi      |            100223   |           96002.1 |  -0.0421141   |            49 |   0.142857 |    -0.0414742  |        0.129107 |
|   2023 | v2_c_daily_core_4h_dual_bear_rsi      |             96002.1 |           95689.7 |  -0.00325385  |            12 |   0.25     |    -0.00896989 |        0.732528 |
|   2024 | v2_c_daily_core_4h_dual_bear_rsi      |             95689.7 |           96247.3 |   0.00582657  |             5 |   0.2      |    -0.00184011 |        4.13868  |
|   2025 | v2_c_daily_core_4h_dual_bear_rsi      |             96247.3 |           93486.6 |  -0.0286834   |            26 |   0.153846 |    -0.0304241  |        0.14135  |
|   2026 | v2_c_daily_core_4h_dual_bear_rsi      |             93486.6 |           94095.3 |   0.00651116  |            11 |   0.454545 |    -0.00356696 |        2.25119  |
|   2020 | v2_d_daily_core_4h_dual_bear_volume   |            100000   |           99674.5 |  -0.00325536  |             2 |   0        |    -0.00102022 |        0        |
|   2021 | v2_d_daily_core_4h_dual_bear_volume   |             99674.5 |           99077.5 |  -0.00598962  |            11 |   0.363636 |    -0.0096968  |        0.555561 |
|   2022 | v2_d_daily_core_4h_dual_bear_volume   |             99077.5 |           99599.9 |   0.00527271  |            49 |   0.265306 |    -0.0192578  |        1.0868   |
|   2023 | v2_d_daily_core_4h_dual_bear_volume   |             99599.9 |           99423.9 |  -0.00176653  |            12 |   0.25     |    -0.00657682 |        0.780711 |
|   2024 | v2_d_daily_core_4h_dual_bear_volume   |             99423.9 |           99833   |   0.00411414  |            10 |   0.3      |    -0.00435266 |        1.62313  |
|   2025 | v2_d_daily_core_4h_dual_bear_volume   |             99833   |           99803.9 |  -0.000291375 |            28 |   0.142857 |    -0.0233666  |        0.992937 |
|   2026 | v2_d_daily_core_4h_dual_bear_volume   |             99803.9 |          101437   |   0.0163649   |            11 |   0.272727 |    -0.00824131 |        2.26663  |
|   2021 | v2_e_daily_volume_4h_dual_bear_volume |            100000   |          100155   |   0.00154592  |             2 |   0.5      |     0          |        4.29161  |
|   2022 | v2_e_daily_volume_4h_dual_bear_volume |            100155   |          100661   |   0.00505961  |            18 |   0.388889 |    -0.00779834 |        1.26974  |
|   2023 | v2_e_daily_volume_4h_dual_bear_volume |            100661   |          100419   |  -0.00240976  |             3 |   0        |    -0.00195521 |        0        |
|   2024 | v2_e_daily_volume_4h_dual_bear_volume |            100419   |          101265   |   0.00842526  |             7 |   0.428571 |    -0.00143712 |        4.64089  |
|   2025 | v2_e_daily_volume_4h_dual_bear_volume |            101265   |          102134   |   0.0085815   |            15 |   0.2      |    -0.0126367  |        1.41742  |
|   2026 | v2_e_daily_volume_4h_dual_bear_volume |            102134   |          103502   |   0.0133966   |             7 |   0.142857 |    -0.00937458 |        2.39693  |
|   2020 | v2_f_daily_ema55_4h_ema55_pullback    |            100000   |           99135.8 |  -0.00864161  |             3 |   0        |    -0.00641855 |        0        |
|   2021 | v2_f_daily_ema55_4h_ema55_pullback    |             99135.8 |          100456   |   0.0133143   |            11 |   0.636364 |    -0.00452    |        3.73901  |
|   2022 | v2_f_daily_ema55_4h_ema55_pullback    |            100456   |           99387.7 |  -0.0106317   |            24 |   0.25     |    -0.0158538  |        0.548456 |
|   2023 | v2_f_daily_ema55_4h_ema55_pullback    |             99387.7 |           99025.8 |  -0.0036417   |             4 |   0.25     |    -0.00507381 |        0.418174 |
|   2024 | v2_f_daily_ema55_4h_ema55_pullback    |             99025.8 |           98666   |  -0.00363312  |             3 |   0        |    -0.00347127 |        0        |
|   2025 | v2_f_daily_ema55_4h_ema55_pullback    |             98666   |           97066.1 |  -0.0162159   |            22 |   0.136364 |    -0.0180156  |        0.34859  |
|   2026 | v2_f_daily_ema55_4h_ema55_pullback    |             97066.1 |           97210   |   0.00148293  |             5 |   0.2      |    -0.00203979 |        1.33027  |

## 7. Findings

- best overall: v2_e_daily_volume_4h_dual_bear_volume
- best overall metrics: profit_factor=1.64, total_return=3.50%, max_drawdown=-1.26%, trade_count=52
- best low drawdown: v2_e_daily_volume_4h_dual_bear_volume with max_drawdown -1.26%
- highest return: v2_e_daily_volume_4h_dual_bear_volume

## 8. Rejected Or Weak Candidates

- v2_d_daily_core_4h_dual_bear_volume: profit_factor 1.10 <= 1.30 continuation threshold
- v2_a_daily_core_4h_ema21_pullback: profit_factor 0.79 <= 1.00; average_R -0.05 <= 0
- v2_f_daily_ema55_4h_ema55_pullback: profit_factor 0.63 <= 1.00; average_R -0.08 <= 0
- v2_b_daily_core_4h_ema55_pullback: profit_factor 0.48 <= 1.00; average_R -0.12 <= 0
- v2_c_daily_core_4h_dual_bear_rsi: profit_factor 0.45 <= 1.00; average_R -0.10 <= 0

## 9. Recommendation

Primary V2 direction: `v2_e_daily_volume_4h_dual_bear_volume`

Why this one:
- It scored highest after penalizing low sample size.
- It kept profit factor above 1.3 with at least 30 trades: yes
- It produced a drawdown profile of -1.26%.

Operational reading:
- Daily bars define whether BTC is already in a bearish regime.
- 4H bars are only used to time the actual short entry once the daily bias is aligned.
- If this V2 still has modest absolute return, the next step should be improving exits before adding more entry complexity.

## 10. Output Notes

- total trades exported: 663
- results_dir: `results_v2_daily_4h`