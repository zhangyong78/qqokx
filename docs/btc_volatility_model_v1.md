# BTC 本地波动率模型 v1

入口：**AI → 样本预测 → BTC 波动率模型**。旧的 BTC 周线样本功能在另一页签保留。
点击“刷新波动率预测”，只读已有 BTC-USDT 现货 1H 与 BTC DVOL 小时缓存；不联网、不下单、不调用大模型。

## 输出与用途

- 主模型：未来1日的实际波动率 RV，年化百分比；辅助模型：未来7日平均实际方差对应的年化 RV。
- 波动尺度：`年化RV × sqrt(天数 / 365)`，可辅助风险预算和仓位敏感度判断。这不是高低点预测、涨跌预测，也不是校准过的置信区间。
- 图表及最近20日表格对比冻结模型1日预测与同期限实际值；未完成日显示“待完成”。
- 数据必须是北京时间完整日、24个连续且已确认的现货小时收益。当天数据不入模；历史缺日停止预测，数据过期明确标记，绝不改写旧结果为今日预测。
- DVOL 是30日隐含波动率，1/7日 RV 与其期限不同，不能直接相减后据此买卖期权。此版本是波动风险估计器，不是已验证盈利的期权策略。

## 固定模型与验证

版本 `btc-rv-1.0-20260930`。2021～2023拟合，2024选模型及正则参数；使用标签截止2024-12-31的数据重拟合一次后冻结。2025～2026-09-29为历史测试，不自动用测试段或新数据重训。

| 输出 | 模型 | 测试样本 | QLIKE / EWMA基准 | 年化MAE / 基准（百分点） | 相对评分 |
|---|---|---:|---:|---:|---:|
| 1日主模型 | integrated_ridge100 | 636 | 0.26065 / 0.41991 | 10.76 / 14.90 | 68.96 |
| 7日辅助 | har_iv_ridge100 | 630 | 0.15693 / 0.20299 | 9.58 / 10.59 | 61.34 |

评分为 `clip(50 + 50 × (1 - 模型QLIKE / 基准QLIKE), 0, 100)`，50表示等于基准，不是胜率或盈利率。1日模型同时优于补充日历/ATR基准；7日相对更强基准的优势不够稳定，因此只作辅助。历史验证不保证未来有效，未证明可交易利润。

1日输入：最近1/7/30日实际方差、DVOL隐含方差、DVOL日变化、BTC最近1/7日收益、ATR14、标准化日振幅、目标日是否周末。7日使用前四类方差指标。使用标准化岭回归预测对数方差，按研究版 smearing 参数还原方差。

权重内置于 `okx_quant/volatility_model_weights.py`，运行不依赖 `research` 或 `reports` 目录，不增加依赖。

## 程序调用

```python
from okx_quant.volatility_prediction import predict_local

result = predict_local()
if result["is_current"]:
    next_day = result["forecasts"][0]
    print(next_day["target_end"], next_day["annualized_rv_percent"])
else:
    print(result["status"])  # 必须先更新缓存，不能拿过期结果作当前预测
```

PowerShell 在项目目录直接输出 JSON：

```powershell
.\.venv\Scripts\python.exe -m okx_quant.volatility_prediction
```

核验研究来源：`reports/btc_volatility_validation_20260930/模型验证报告.md`。
