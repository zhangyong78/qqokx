# V5 Combo Filter Report

## 1. Study Goal

Formalize the strongest V4 single filters and test whether combining them improves robustness without killing sample size.

## 2. Fixed Trade Logic

- entry trigger: 4H dual-bear pullback with 4H volume confirmation
- exit rule: EMA21 reclaim on 4H close, execute on next 4H open
- only the daily regime gate changes

## 3. Data Source

- source: `local_okx_candle_cache`
- data_root: `D:\qqokx_data`
- symbol: `BTC-USDT-SWAP`
- daily_timeframe: `1D`
- entry_timeframe: `4H`

## 4. Daily Gates Compared

- `v5_a_baseline`: baseline V2 daily regime
- `v5_b_close_below_ema21`: best V4 return-oriented single filter
- `v5_c_rsi_rebound`: best V4 quality-oriented single filter
- `v5_d_close_and_rsi`: close below EMA21 plus RSI rebound
- `v5_e_close_rsi_ema55`: close below EMA21 plus RSI rebound plus EMA55 slope down
- `v5_f_close_breakdown`: close below EMA21 plus breakdown-and-slope state

## 5. Data Range

- daily first bar: 2019-11-27T16:00:00+00:00
- daily last bar: 2026-06-01T16:00:00+00:00
- 4H first bar: 2019-12-16T04:00:00+00:00
- 4H last bar: 2026-06-02T12:00:00+00:00

## 6. Strategy Summary

| strategy_name          | start_date                | end_date                  |   initial_capital |   final_equity |   total_return |   annual_return |   max_drawdown |   profit_factor |   win_rate |   trade_count |   average_R |    median_R |   largest_win_R |   largest_loss_R |   average_holding_days |   max_consecutive_losses |   sharpe |   sortino |   calmar |   score |
|:-----------------------|:--------------------------|:--------------------------|------------------:|---------------:|---------------:|----------------:|---------------:|----------------:|-----------:|--------------:|------------:|------------:|----------------:|-----------------:|-----------------------:|-------------------------:|---------:|----------:|---------:|--------:|
| v5_e_close_rsi_ema55   | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         103553 |      0.0355343 |      0.0054187  |    -0.00787381 |         2.27574 |   0.333333 |            30 |    0.235829 | -0.15995    |         4.59734 |        -0.535881 |                2.34444 |                        5 | 0.186835 | 0.0648201 | 0.688193 | 6.62877 |
| v5_d_close_and_rsi     | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         103562 |      0.0356186 |      0.00543137 |    -0.00787381 |         2.2787  |   0.354839 |            31 |    0.228747 | -0.146893   |         4.59734 |        -0.535881 |                2.4086  |                        5 | 0.187264 | 0.0649693 | 0.689802 | 6.60795 |
| v5_c_rsi_rebound       | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         103162 |      0.0316153 |      0.00482887 |    -0.00962761 |         1.99563 |   0.305556 |            36 |    0.17547  | -0.15945    |         4.59734 |        -0.535881 |                2.28241 |                        7 | 0.166466 | 0.0587859 | 0.501565 | 5.67181 |
| v5_b_close_below_ema21 | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         103904 |      0.0390372 |      0.00594433 |    -0.0108883  |         1.76927 |   0.319149 |            47 |    0.165541 | -0.173006   |         4.59734 |        -0.848008 |                2.40071 |                        7 | 0.1783   | 0.0527674 | 0.545939 | 5.06756 |
| v5_a_baseline          | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         103502 |      0.0350207 |      0.00534152 |    -0.0126367  |         1.64119 |   0.288462 |            52 |    0.134734 | -0.172506   |         4.59734 |        -0.848008 |                2.3141  |                        9 | 0.160301 | 0.0484633 | 0.422697 | 4.60881 |
| v5_f_close_breakdown   | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         103698 |      0.0369817 |      0.00563607 |    -0.00415946 |         5.03648 |   0.5      |            12 |    0.609493 |  0.00325366 |         2.66125 |        -0.69956  |                4.09722 |                        3 | 0.258875 | 0.0509014 | 1.355    | 1.98324 |

## 7. Yearly Performance

|   year | strategy_name          |   year_start_equity |   year_end_equity |   year_return |   trade_count |   win_rate |   max_drawdown |   profit_factor |
|-------:|:-----------------------|--------------------:|------------------:|--------------:|--------------:|-----------:|---------------:|----------------:|
|   2021 | v5_a_baseline          |              100000 |            100155 |    0.00154592 |             2 |   0.5      |    0           |         4.29161 |
|   2022 | v5_a_baseline          |              100155 |            100661 |    0.00505961 |            18 |   0.388889 |   -0.00779834  |         1.26974 |
|   2023 | v5_a_baseline          |              100661 |            100419 |   -0.00240976 |             3 |   0        |   -0.00195521  |         0       |
|   2024 | v5_a_baseline          |              100419 |            101265 |    0.00842526 |             7 |   0.428571 |   -0.00143712  |         4.64089 |
|   2025 | v5_a_baseline          |              101265 |            102134 |    0.0085815  |            15 |   0.2      |   -0.0126367   |         1.41742 |
|   2026 | v5_a_baseline          |              102134 |            103502 |    0.0133966  |             7 |   0.142857 |   -0.00937458  |         2.39693 |
|   2021 | v5_b_close_below_ema21 |              100000 |            100155 |    0.00154592 |             2 |   0.5      |    0           |         4.29161 |
|   2022 | v5_b_close_below_ema21 |              100155 |            100786 |    0.00630797 |            17 |   0.411765 |   -0.00779834  |         1.36027 |
|   2023 | v5_b_close_below_ema21 |              100786 |            100543 |   -0.00240976 |             3 |   0        |   -0.00195521  |         0       |
|   2024 | v5_b_close_below_ema21 |              100543 |            101391 |    0.00842741 |             6 |   0.5      |   -0.00143712  |         4.64517 |
|   2025 | v5_b_close_below_ema21 |              101391 |            102442 |    0.0103675  |            13 |   0.230769 |   -0.0108883   |         1.55161 |
|   2026 | v5_b_close_below_ema21 |              102442 |            103904 |    0.0142689  |             6 |   0.166667 |   -0.00852188  |         2.63676 |
|   2021 | v5_c_rsi_rebound       |              100000 |            100202 |    0.00201652 |             1 |   1        |    0           |       999       |
|   2022 | v5_c_rsi_rebound       |              100202 |            100314 |    0.00112426 |            11 |   0.363636 |   -0.00628807  |         1.11757 |
|   2023 | v5_c_rsi_rebound       |              100314 |            100140 |   -0.00173537 |             2 |   0        |   -0.00128051  |         0       |
|   2024 | v5_c_rsi_rebound       |              100140 |            101129 |    0.00987658 |             5 |   0.6      |   -2.13206e-06 |        12.3896  |
|   2025 | v5_c_rsi_rebound       |              101129 |            101556 |    0.0042162  |            11 |   0.181818 |   -0.00962761  |         1.34728 |
|   2026 | v5_c_rsi_rebound       |              101556 |            103162 |    0.0158128  |             6 |   0.166667 |   -0.00701266  |         3.20423 |
|   2021 | v5_d_close_and_rsi     |              100000 |            100202 |    0.00201652 |             1 |   1        |    0           |       999       |
|   2022 | v5_d_close_and_rsi     |              100202 |            100439 |    0.00236773 |            10 |   0.4      |   -0.0050538   |         1.28462 |
|   2023 | v5_d_close_and_rsi     |              100439 |            100265 |   -0.00173537 |             2 |   0        |   -0.00128051  |         0       |
|   2024 | v5_d_close_and_rsi     |              100265 |            101255 |    0.00987873 |             4 |   0.75     |    0           |        12.4201  |
|   2025 | v5_d_close_and_rsi     |              101255 |            101862 |    0.00599451 |             9 |   0.222222 |   -0.00787381  |         1.57849 |
|   2026 | v5_d_close_and_rsi     |              101862 |            103562 |    0.0166872  |             5 |   0.2      |   -0.00615793  |         3.64898 |
|   2021 | v5_e_close_rsi_ema55   |              100000 |            100202 |    0.00201652 |             1 |   1        |    0           |       999       |
|   2022 | v5_e_close_rsi_ema55   |              100202 |            100439 |    0.00236773 |            10 |   0.4      |   -0.0050538   |         1.28462 |
|   2023 | v5_e_close_rsi_ema55   |              100439 |            100265 |   -0.00173537 |             2 |   0        |   -0.00128051  |         0       |
|   2024 | v5_e_close_rsi_ema55   |              100265 |            101247 |    0.0097965  |             3 |   0.666667 |    0           |        12.325   |
|   2025 | v5_e_close_rsi_ema55   |              101247 |            101854 |    0.00599451 |             9 |   0.222222 |   -0.00787381  |         1.57849 |
|   2026 | v5_e_close_rsi_ema55   |              101854 |            103553 |    0.0166872  |             5 |   0.2      |   -0.00615793  |         3.64898 |
|   2022 | v5_f_close_breakdown   |              100000 |            101601 |    0.0160061  |             5 |   0.8      |   -0.0034978   |         5.49739 |
|   2023 | v5_f_close_breakdown   |              101601 |            101432 |   -0.00165539 |             2 |   0        |   -0.0012005   |         0       |
|   2024 | v5_f_close_breakdown   |              101432 |            101326 |   -0.00105373 |             1 |   0        |    0           |         0       |
|   2025 | v5_f_close_breakdown   |              101326 |            103698 |    0.0234159  |             4 |   0.5      |   -0.0017216   |         9.31866 |

## 8. Findings

- best combo gate: v5_e_close_rsi_ema55
- best metrics: profit_factor=2.28, total_return=3.55%, max_drawdown=-0.79%, trade_count=30, average_R=0.24
- baseline metrics: profit_factor=1.64, total_return=3.50%, max_drawdown=-1.26%, trade_count=52, average_R=0.13
- return delta vs baseline: 0.05%
- drawdown delta vs baseline: 0.48%

## 9. Rejected Or Weak Filters

- v5_f_close_breakdown: trade_count 12 < 30

## 10. Recommendation

Best daily combo gate to carry forward: `v5_e_close_rsi_ema55`

Interpretation:
- If the combo beats both strong single filters, the environment edge likely comes from conditional context rather than one variable alone.
- If the combo loses to singles, the regime logic is probably better kept simple.
- If the combo survives with around 30 or more trades and better PF, it is a credible V6 foundation.

## 11. Output Notes

- total trades exported: 208
- results_dir: `results_v5_combo_study`