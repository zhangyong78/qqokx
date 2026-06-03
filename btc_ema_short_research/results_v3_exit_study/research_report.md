# V3 Exit Study Report

## 1. Study Goal

Keep the best V2 entry framework fixed and compare whether changing only the exit rule can improve expectancy, drawdown, and yearly stability.

## 2. Fixed Entry Framework

- entry_strategy: `v2_e_daily_volume_4h_dual_bear_volume`
- daily regime: daily EMA21 < EMA55, daily close < EMA55, daily volume >= daily VOL_MA20
- entry trigger: 4H dual-bear pullback with 4H volume confirmation

## 3. Data Source

- source: `local_okx_candle_cache`
- data_root: `D:\qqokx_data`
- symbol: `BTC-USDT-SWAP`
- daily_timeframe: `1D`
- entry_timeframe: `4H`

## 4. Data Range

- daily first bar: 2019-11-27T16:00:00+00:00
- daily last bar: 2026-06-01T16:00:00+00:00
- 4H first bar: 2019-12-16T04:00:00+00:00
- 4H last bar: 2026-06-02T12:00:00+00:00

## 5. Exit Rules Compared

- `v3_exit_ema21_reclaim`: baseline V2 exit
- `v3_exit_fixed_1_5R`: fixed 1.5R take profit
- `v3_exit_fixed_2R`: fixed 2R take profit
- `v3_exit_ema21_or_2R`: first of EMA21 reclaim or 2R target
- `v3_exit_atr_trail_2ATR`: 2ATR trailing stop
- `v3_exit_atr_trail_1_5ATR_or_2R`: first of 1.5ATR trail or 2R target

## 6. Strategy Summary

| strategy_name                  | start_date                | end_date                  |   initial_capital |   final_equity |   total_return |   annual_return |   max_drawdown |   profit_factor |   win_rate |   trade_count |   average_R |   median_R |   largest_win_R |   largest_loss_R |   average_holding_days |   max_consecutive_losses |     sharpe |     sortino |     calmar | entry_strategy_name                   | exit_rule_kind       |    score |
|:-------------------------------|:--------------------------|:--------------------------|------------------:|---------------:|---------------:|----------------:|---------------:|----------------:|-----------:|--------------:|------------:|-----------:|----------------:|-----------------:|-----------------------:|-------------------------:|-----------:|------------:|-----------:|:--------------------------------------|:---------------------|---------:|
| v3_exit_ema21_reclaim          | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |       103502   |     0.0350207  |     0.00534152  |     -0.0126367 |        1.64119  |   0.288462 |            52 |   0.134734  |  -0.172506 |         4.59734 |        -0.848008 |                2.3141  |                        9 |  0.160301  |  0.0484633  |  0.422697  | v2_e_daily_volume_4h_dual_bear_volume | ema21_reclaim        |  4.60881 |
| v3_exit_ema21_or_2R            | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |       102923   |     0.0292298  |     0.00446891  |     -0.0126367 |        1.53333  |   0.301887 |            53 |   0.110278  |  -0.172006 |         1.99181 |        -0.848008 |                1.83962 |                        9 |  0.162818  |  0.0404294  |  0.353644  | v2_e_daily_volume_4h_dual_bear_volume | ema21_or_fixed_r     |  4.23437 |
| v3_exit_atr_trail_1_5ATR_or_2R | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |       101456   |     0.014557   |     0.0022392   |     -0.0092474 |        1.28106  |   0.344828 |            58 |   0.0506494 |  -0.125966 |         1.97248 |        -0.551356 |                1.17529 |                        4 |  0.108269  |  0.0251573  |  0.242144  | v2_e_daily_volume_4h_dual_bear_volume | atr_trail_or_fixed_r |  3.36767 |
| v3_exit_atr_trail_2ATR         | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |       100568   |     0.00567918 |     0.000876847 |     -0.0153084 |        1.08893  |   0.37037  |            54 |   0.0220225 |  -0.17315  |         2.33378 |        -0.788128 |                1.96914 |                        4 |  0.0400624 |  0.00757368 |  0.0572789 | v2_e_daily_volume_4h_dual_bear_volume | atr_trail            |  2.72558 |
| v3_exit_fixed_1_5R             | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |        96215.1 |    -0.0378491  |    -0.00595373  |     -0.0488169 |        0.656773 |   0.3125   |            32 |  -0.237651  |  -1.01473  |         1.48903 |        -1.05107  |               11.4531  |                        7 | -0.182371  | -1.08648    | -0.12196   | v2_e_daily_volume_4h_dual_bear_volume | fixed_r              | -2.6492  |
| v3_exit_fixed_2R               | 2019-12-16T04:00:00+00:00 | 2026-06-02T12:00:00+00:00 |            100000 |        95752.3 |    -0.0424773  |    -0.00669528  |     -0.0503818 |        0.612065 |   0.241379 |            29 |  -0.295018  |  -1.01554  |         1.98933 |        -1.05107  |               13.3391  |                        9 | -0.19354   | -1.23507    | -0.132891  | v2_e_daily_volume_4h_dual_bear_volume | fixed_r              | -6.00557 |

## 7. Yearly Performance

|   year | strategy_name                  |   year_start_equity |   year_end_equity |   year_return |   trade_count |   win_rate |   max_drawdown |   profit_factor | entry_strategy_name                   |
|-------:|:-------------------------------|--------------------:|------------------:|--------------:|--------------:|-----------:|---------------:|----------------:|:--------------------------------------|
|   2021 | v3_exit_ema21_reclaim          |            100000   |          100155   |   0.00154592  |             2 |   0.5      |    0           |        4.29161  | v2_e_daily_volume_4h_dual_bear_volume |
|   2022 | v3_exit_ema21_reclaim          |            100155   |          100661   |   0.00505961  |            18 |   0.388889 |   -0.00779834  |        1.26974  | v2_e_daily_volume_4h_dual_bear_volume |
|   2023 | v3_exit_ema21_reclaim          |            100661   |          100419   |  -0.00240976  |             3 |   0        |   -0.00195521  |        0        | v2_e_daily_volume_4h_dual_bear_volume |
|   2024 | v3_exit_ema21_reclaim          |            100419   |          101265   |   0.00842526  |             7 |   0.428571 |   -0.00143712  |        4.64089  | v2_e_daily_volume_4h_dual_bear_volume |
|   2025 | v3_exit_ema21_reclaim          |            101265   |          102134   |   0.0085815   |            15 |   0.2      |   -0.0126367   |        1.41742  | v2_e_daily_volume_4h_dual_bear_volume |
|   2026 | v3_exit_ema21_reclaim          |            102134   |          103502   |   0.0133966   |             7 |   0.142857 |   -0.00937458  |        2.39693  | v2_e_daily_volume_4h_dual_bear_volume |
|   2021 | v3_exit_fixed_1_5R             |            100000   |          100232   |   0.0023238   |             2 |   0.5      |    0           |        1.46249  | v2_e_daily_volume_4h_dual_bear_volume |
|   2022 | v3_exit_fixed_1_5R             |            100232   |           98389.4 |  -0.0183875   |            11 |   0.272727 |   -0.0256418   |        0.546371 | v2_e_daily_volume_4h_dual_bear_volume |
|   2023 | v3_exit_fixed_1_5R             |             98389.4 |           96880.2 |  -0.015339    |             3 |   0        |   -0.0102226   |        0        | v2_e_daily_volume_4h_dual_bear_volume |
|   2024 | v3_exit_fixed_1_5R             |             96880.2 |           96827.7 |  -0.000541689 |             5 |   0.4      |   -0.00510283  |        0.964543 | v2_e_daily_volume_4h_dual_bear_volume |
|   2025 | v3_exit_fixed_1_5R             |             96827.7 |           97480.4 |   0.00674075  |             6 |   0.5      |   -0.0152886   |        1.43768  | v2_e_daily_volume_4h_dual_bear_volume |
|   2026 | v3_exit_fixed_1_5R             |             97480.4 |           96215.1 |  -0.0129798   |             5 |   0.2      |   -0.0151971   |        0.361479 | v2_e_daily_volume_4h_dual_bear_volume |
|   2021 | v3_exit_fixed_2R               |            100000   |           98987.9 |  -0.0101209   |             2 |   0        |   -0.00512202  |        0        | v2_e_daily_volume_4h_dual_bear_volume |
|   2022 | v3_exit_fixed_2R               |             98987.9 |           96934.3 |  -0.0207465   |            10 |   0.2      |   -0.0303909   |        0.488785 | v2_e_daily_volume_4h_dual_bear_volume |
|   2023 | v3_exit_fixed_2R               |             96934.3 |           95447.4 |  -0.015339    |             3 |   0        |   -0.0102226   |        0        | v2_e_daily_volume_4h_dual_bear_volume |
|   2024 | v3_exit_fixed_2R               |             95447.4 |           96356.9 |   0.00952893  |             4 |   0.5      |   -0.00510283  |        1.9329   | v2_e_daily_volume_4h_dual_bear_volume |
|   2025 | v3_exit_fixed_2R               |             96356.9 |           96771.2 |   0.00429927  |             5 |   0.4      |   -0.0152886   |        1.27846  | v2_e_daily_volume_4h_dual_bear_volume |
|   2026 | v3_exit_fixed_2R               |             96771.2 |           95752.3 |  -0.0105289   |             5 |   0.2      |   -0.0151971   |        0.483011 | v2_e_daily_volume_4h_dual_bear_volume |
|   2021 | v3_exit_ema21_or_2R            |            100000   |          100155   |   0.00154592  |             2 |   0.5      |    0           |        4.29161  | v2_e_daily_volume_4h_dual_bear_volume |
|   2022 | v3_exit_ema21_or_2R            |            100155   |          101538   |   0.0138102   |            18 |   0.388889 |   -0.00779834  |        1.73457  | v2_e_daily_volume_4h_dual_bear_volume |
|   2023 | v3_exit_ema21_or_2R            |            101538   |          101293   |  -0.00240976  |             3 |   0        |   -0.00195521  |        0        | v2_e_daily_volume_4h_dual_bear_volume |
|   2024 | v3_exit_ema21_or_2R            |            101293   |          102370   |   0.0106316   |             7 |   0.428571 |   -0.00143712  |        5.58808  | v2_e_daily_volume_4h_dual_bear_volume |
|   2025 | v3_exit_ema21_or_2R            |            102370   |          102880   |   0.00497987  |            16 |   0.25     |   -0.0126367   |        1.24307  | v2_e_daily_volume_4h_dual_bear_volume |
|   2026 | v3_exit_ema21_or_2R            |            102880   |          102923   |   0.000420001 |             7 |   0.142857 |   -0.00937458  |        1.04436  | v2_e_daily_volume_4h_dual_bear_volume |
|   2021 | v3_exit_atr_trail_2ATR         |            100000   |          100271   |   0.0027062   |             2 |   1        |    0           |      999        | v2_e_daily_volume_4h_dual_bear_volume |
|   2022 | v3_exit_atr_trail_2ATR         |            100271   |          100449   |   0.00177923  |            20 |   0.35     |   -0.00624494  |        1.07661  | v2_e_daily_volume_4h_dual_bear_volume |
|   2023 | v3_exit_atr_trail_2ATR         |            100449   |          100518   |   0.00068906  |             2 |   0.5      |   -0.000914561 |        1.75223  | v2_e_daily_volume_4h_dual_bear_volume |
|   2024 | v3_exit_atr_trail_2ATR         |            100518   |          101250   |   0.00728307  |             7 |   0.428571 |   -0.00454717  |        1.87979  | v2_e_daily_volume_4h_dual_bear_volume |
|   2025 | v3_exit_atr_trail_2ATR         |            101250   |          101116   |  -0.00132964  |            16 |   0.3125   |   -0.0116232   |        0.932006 | v2_e_daily_volume_4h_dual_bear_volume |
|   2026 | v3_exit_atr_trail_2ATR         |            101116   |          100568   |  -0.00541731  |             7 |   0.285714 |   -0.0111167   |        0.525023 | v2_e_daily_volume_4h_dual_bear_volume |
|   2021 | v3_exit_atr_trail_1_5ATR_or_2R |            100000   |          100299   |   0.00298929  |             2 |   0.5      |    0           |        5.94904  | v2_e_daily_volume_4h_dual_bear_volume |
|   2022 | v3_exit_atr_trail_1_5ATR_or_2R |            100299   |          101185   |   0.00883293  |            21 |   0.380952 |   -0.00516521  |        1.4979   | v2_e_daily_volume_4h_dual_bear_volume |
|   2023 | v3_exit_atr_trail_1_5ATR_or_2R |            101185   |          101168   |  -0.000162354 |             3 |   0.333333 |   -0.00253583  |        0.936128 | v2_e_daily_volume_4h_dual_bear_volume |
|   2024 | v3_exit_atr_trail_1_5ATR_or_2R |            101168   |          100879   |  -0.00286372  |             8 |   0.25     |   -0.00316721  |        0.489194 | v2_e_daily_volume_4h_dual_bear_volume |
|   2025 | v3_exit_atr_trail_1_5ATR_or_2R |            100879   |          101744   |   0.00857488  |            17 |   0.352941 |   -0.00584525  |        1.59372  | v2_e_daily_volume_4h_dual_bear_volume |
|   2026 | v3_exit_atr_trail_1_5ATR_or_2R |            101744   |          101456   |  -0.00283107  |             7 |   0.285714 |   -0.0092474   |        0.727821 | v2_e_daily_volume_4h_dual_bear_volume |

## 8. Findings

- best overall exit: v3_exit_ema21_reclaim
- best metrics: profit_factor=1.64, total_return=3.50%, max_drawdown=-1.26%, trade_count=52, average_R=0.13
- baseline metrics: profit_factor=1.64, total_return=3.50%, max_drawdown=-1.26%, trade_count=52, average_R=0.13
- return delta vs baseline: 0.00%
- drawdown delta vs baseline: 0.00%

## 9. Rejected Or Weak Exit Rules

- v3_exit_atr_trail_1_5ATR_or_2R: profit_factor 1.28 <= 1.30 continuation threshold
- v3_exit_atr_trail_2ATR: profit_factor 1.09 <= 1.30 continuation threshold
- v3_exit_fixed_1_5R: profit_factor 0.66 <= 1.00; average_R -0.24 <= 0
- v3_exit_fixed_2R: trade_count 29 < 30; profit_factor 0.61 <= 1.00; average_R -0.30 <= 0

## 10. Recommendation

Best exit rule to carry forward: `v3_exit_ema21_reclaim`

Interpretation:
- If the best rule clearly beats baseline, the bottleneck was largely exit design rather than entry quality.
- If the best rule only marginally improves results, then both entry and exit are already close to the ceiling of this setup.
- If no rule is eligible, this 1D + 4H short framework likely needs a different regime filter rather than more exit polishing.

## 11. Output Notes

- total trades exported: 278
- results_dir: `results_v3_exit_study`