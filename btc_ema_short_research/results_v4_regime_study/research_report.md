# V4 Regime Filter Report

## 1. Study Goal

Keep the best V2 entry and V3 baseline exit fixed, then compare which daily environment filters improve or damage the 1D+4H short framework.

## 2. Fixed Trade Logic

- entry trigger: 4H dual-bear pullback with 4H volume confirmation
- exit rule: EMA21 reclaim on 4H close, execute on next 4H open
- the only thing changing in this study is the daily regime gate

## 3. Data Source

- source: `local_okx_candle_cache`
- data_root: `D:\qqokx_data`
- symbol: `BTC-USDT-SWAP`
- daily_timeframe: `1D`
- entry_timeframe: `4H`

## 4. Daily Environment Filters Compared

- `v4_a_baseline`: daily core bear regime plus daily volume >= VOL_MA20
- `v4_b_close_below_ema21`: baseline plus daily close < EMA21
- `v4_c_ema55_down`: baseline plus daily EMA55 slope < 0
- `v4_d_rsi_rebound`: baseline plus daily RSI inside rebound window
- `v4_e_atr_expansion`: baseline plus daily ATR above rolling 100-bar median
- `v4_f_volume_strong`: baseline plus stronger daily volume expansion
- `v4_g_trend_gap_strong`: baseline plus stronger EMA21/EMA55 separation
- `v4_h_breakdown_and_slope`: baseline plus daily close < previous low and EMA55 slope < 0

## 5. Data Range

- daily first bar: 2019-11-27T16:00:00+00:00
- daily last bar: 2026-06-01T16:00:00+00:00
- 4H first bar: 2019-12-16T04:00:00+00:00
- 4H last bar: 2026-06-02T12:00:00+00:00

## 6. Strategy Summary

| strategy_name            | start_date                | end_date                  |   initial_capital |   final_equity |   total_return |   annual_return |   max_drawdown |   profit_factor |   win_rate |   trade_count |   average_R |    median_R |   largest_win_R |   largest_loss_R |   average_holding_days |   max_consecutive_losses |    sharpe |   sortino |   calmar |     score |
|:-------------------------|:--------------------------|:--------------------------|------------------:|---------------:|---------------:|----------------:|---------------:|----------------:|-----------:|--------------:|------------:|------------:|----------------:|-----------------:|-----------------------:|-------------------------:|----------:|----------:|---------:|----------:|
| v4_d_rsi_rebound         | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         103162 |      0.0316153 |      0.00482887 |    -0.00962761 |         1.99563 |   0.305556 |            36 |    0.17547  | -0.15945    |         4.59734 |        -0.535881 |                2.28241 |                        7 | 0.166466  | 0.0587859 | 0.501565 |  5.67181  |
| v4_b_close_below_ema21   | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         103904 |      0.0390372 |      0.00594433 |    -0.0108883  |         1.76927 |   0.319149 |            47 |    0.165541 | -0.173006   |         4.59734 |        -0.848008 |                2.40071 |                        7 | 0.1783    | 0.0527674 | 0.545939 |  5.06756  |
| v4_c_ema55_down          | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         103494 |      0.0349364 |      0.00532885 |    -0.0126367  |         1.63967 |   0.27451  |            51 |    0.137057 | -0.173006   |         4.59734 |        -0.848008 |                2.27451 |                        9 | 0.159929  | 0.0483506 | 0.421695 |  4.61422  |
| v4_a_baseline            | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         103502 |      0.0350207 |      0.00534152 |    -0.0126367  |         1.64119 |   0.288462 |            52 |    0.134734 | -0.172506   |         4.59734 |        -0.848008 |                2.3141  |                        9 | 0.160301  | 0.0484633 | 0.422697 |  4.60881  |
| v4_g_trend_gap_strong    | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         102593 |      0.0259318 |      0.0039701  |    -0.00937458 |         1.51984 |   0.297872 |            47 |    0.111142 | -0.207226   |         4.59734 |        -0.848008 |                2.33333 |                        7 | 0.12973   | 0.0367509 | 0.423496 |  4.21967  |
| v4_h_breakdown_and_slope | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         103698 |      0.0369817 |      0.00563607 |    -0.00415946 |         5.03648 |   0.5      |            12 |    0.609493 |  0.00325366 |         2.66125 |        -0.69956  |                4.09722 |                        3 | 0.258875  | 0.0509014 | 1.355    |  1.98324  |
| v4_f_volume_strong       | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         101508 |      0.0150785 |      0.00231893 |    -0.00755128 |         1.54975 |   0.384615 |            26 |    0.116596 | -0.114521   |         2.55362 |        -0.848008 |                2.55769 |                        4 | 0.124051  | 0.0172479 | 0.30709  |  0.738798 |
| v4_e_atr_expansion       | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |         100863 |      0.0086298 |      0.00133076 |    -0.0058454  |         1.70729 |   0.357143 |            14 |    0.124215 | -0.176109   |         2.55362 |        -0.560331 |                2.5119  |                        4 | 0.0972629 | 0.0164764 | 0.22766  | -0.787692 |

## 7. Yearly Performance

|   year | strategy_name            |   year_start_equity |   year_end_equity |   year_return |   trade_count |   win_rate |   max_drawdown |   profit_factor |
|-------:|:-------------------------|--------------------:|------------------:|--------------:|--------------:|-----------:|---------------:|----------------:|
|   2021 | v4_a_baseline            |              100000 |            100155 |    0.00154592 |             2 |   0.5      |    0           |        4.29161  |
|   2022 | v4_a_baseline            |              100155 |            100661 |    0.00505961 |            18 |   0.388889 |   -0.00779834  |        1.26974  |
|   2023 | v4_a_baseline            |              100661 |            100419 |   -0.00240976 |             3 |   0        |   -0.00195521  |        0        |
|   2024 | v4_a_baseline            |              100419 |            101265 |    0.00842526 |             7 |   0.428571 |   -0.00143712  |        4.64089  |
|   2025 | v4_a_baseline            |              101265 |            102134 |    0.0085815  |            15 |   0.2      |   -0.0126367   |        1.41742  |
|   2026 | v4_a_baseline            |              102134 |            103502 |    0.0133966  |             7 |   0.142857 |   -0.00937458  |        2.39693  |
|   2021 | v4_b_close_below_ema21   |              100000 |            100155 |    0.00154592 |             2 |   0.5      |    0           |        4.29161  |
|   2022 | v4_b_close_below_ema21   |              100155 |            100786 |    0.00630797 |            17 |   0.411765 |   -0.00779834  |        1.36027  |
|   2023 | v4_b_close_below_ema21   |              100786 |            100543 |   -0.00240976 |             3 |   0        |   -0.00195521  |        0        |
|   2024 | v4_b_close_below_ema21   |              100543 |            101391 |    0.00842741 |             6 |   0.5      |   -0.00143712  |        4.64517  |
|   2025 | v4_b_close_below_ema21   |              101391 |            102442 |    0.0103675  |            13 |   0.230769 |   -0.0108883   |        1.55161  |
|   2026 | v4_b_close_below_ema21   |              102442 |            103904 |    0.0142689  |             6 |   0.166667 |   -0.00852188  |        2.63676  |
|   2021 | v4_c_ema55_down          |              100000 |            100155 |    0.00154592 |             2 |   0.5      |    0           |        4.29161  |
|   2022 | v4_c_ema55_down          |              100155 |            100661 |    0.00505961 |            18 |   0.388889 |   -0.00779834  |        1.26974  |
|   2023 | v4_c_ema55_down          |              100661 |            100419 |   -0.00240976 |             3 |   0        |   -0.00195521  |        0        |
|   2024 | v4_c_ema55_down          |              100419 |            101257 |    0.00834315 |             6 |   0.333333 |   -0.00143712  |        4.60541  |
|   2025 | v4_c_ema55_down          |              101257 |            102126 |    0.0085815  |            15 |   0.2      |   -0.0126367   |        1.41742  |
|   2026 | v4_c_ema55_down          |              102126 |            103494 |    0.0133966  |             7 |   0.142857 |   -0.00937458  |        2.39693  |
|   2021 | v4_d_rsi_rebound         |              100000 |            100202 |    0.00201652 |             1 |   1        |    0           |      999        |
|   2022 | v4_d_rsi_rebound         |              100202 |            100314 |    0.00112426 |            11 |   0.363636 |   -0.00628807  |        1.11757  |
|   2023 | v4_d_rsi_rebound         |              100314 |            100140 |   -0.00173537 |             2 |   0        |   -0.00128051  |        0        |
|   2024 | v4_d_rsi_rebound         |              100140 |            101129 |    0.00987658 |             5 |   0.6      |   -2.13206e-06 |       12.3896   |
|   2025 | v4_d_rsi_rebound         |              101129 |            101556 |    0.0042162  |            11 |   0.181818 |   -0.00962761  |        1.34728  |
|   2026 | v4_d_rsi_rebound         |              101556 |            103162 |    0.0158128  |             6 |   0.166667 |   -0.00701266  |        3.20423  |
|   2021 | v4_e_atr_expansion       |              100000 |            100155 |    0.00154592 |             2 |   0.5      |    0           |        4.29161  |
|   2022 | v4_e_atr_expansion       |              100155 |            100044 |   -0.00109979 |             1 |   0        |    0           |        0        |
|   2024 | v4_e_atr_expansion       |              100044 |            100349 |    0.00304669 |             2 |   1        |    0           |      999        |
|   2025 | v4_e_atr_expansion       |              100349 |            101103 |    0.00751592 |             8 |   0.25     |   -0.0058454   |        1.91696  |
|   2026 | v4_e_atr_expansion       |              101103 |            100863 |   -0.0023786  |             1 |   0        |    0           |        0        |
|   2021 | v4_f_volume_strong       |              100000 |            100155 |    0.00154592 |             2 |   0.5      |    0           |        4.29161  |
|   2022 | v4_f_volume_strong       |              100155 |            101212 |    0.0105563  |            11 |   0.545455 |   -0.00379739  |        1.96053  |
|   2023 | v4_f_volume_strong       |              101212 |            101014 |   -0.00195521 |             2 |   0        |   -0.00067556  |        0        |
|   2024 | v4_f_volume_strong       |              101014 |            101313 |    0.00296502 |             1 |   1        |    0           |      999        |
|   2025 | v4_f_volume_strong       |              101313 |            101780 |    0.00460188 |             8 |   0.25     |   -0.00755128  |        1.41429  |
|   2026 | v4_f_volume_strong       |              101780 |            101508 |   -0.00267101 |             2 |   0        |   -0.000293109 |        0        |
|   2021 | v4_g_trend_gap_strong    |              100000 |            100155 |    0.00154592 |             2 |   0.5      |    0           |        4.29161  |
|   2022 | v4_g_trend_gap_strong    |              100155 |            100661 |    0.00505961 |            18 |   0.388889 |   -0.00779834  |        1.26974  |
|   2023 | v4_g_trend_gap_strong    |              100661 |            100419 |   -0.00240976 |             3 |   0        |   -0.00195521  |        0        |
|   2024 | v4_g_trend_gap_strong    |              100419 |            101353 |    0.00930049 |             5 |   0.6      |   -0.00143712  |        7.42227  |
|   2025 | v4_g_trend_gap_strong    |              101353 |            101237 |   -0.00114213 |            12 |   0.166667 |   -0.00914438  |        0.932067 |
|   2026 | v4_g_trend_gap_strong    |              101237 |            102593 |    0.0133966  |             7 |   0.142857 |   -0.00937458  |        2.39693  |
|   2022 | v4_h_breakdown_and_slope |              100000 |            101601 |    0.0160061  |             5 |   0.8      |   -0.0034978   |        5.49739  |
|   2023 | v4_h_breakdown_and_slope |              101601 |            101432 |   -0.00165539 |             2 |   0        |   -0.0012005   |        0        |
|   2024 | v4_h_breakdown_and_slope |              101432 |            101326 |   -0.00105373 |             1 |   0        |    0           |        0        |
|   2025 | v4_h_breakdown_and_slope |              101326 |            103698 |    0.0234159  |             4 |   0.5      |   -0.0017216   |        9.31866  |

## 8. Findings

- best regime filter: v4_d_rsi_rebound
- best metrics: profit_factor=2.00, total_return=3.16%, max_drawdown=-0.96%, trade_count=36
- baseline metrics: profit_factor=1.64, total_return=3.50%, max_drawdown=-1.26%, trade_count=52
- return delta vs baseline: -0.34%
- drawdown delta vs baseline: 0.30%

## 9. Rejected Or Weak Filters

- v4_h_breakdown_and_slope: trade_count 12 < 30
- v4_f_volume_strong: trade_count 26 < 30
- v4_e_atr_expansion: trade_count 14 < 30

## 10. Recommendation

Best daily regime gate to carry forward: `v4_d_rsi_rebound`

Interpretation:
- If the best filter meaningfully beats baseline, daily environment selection is the current edge amplifier.
- If most stricter filters reduce quality, the baseline regime is already close to the useful boundary and over-filtering hurts sample quality.
- If only one or two filters survive while others fail, we should focus V5 on refining just those surviving market states.

## 11. Output Notes

- total trades exported: 285
- results_dir: `results_v4_regime_study`