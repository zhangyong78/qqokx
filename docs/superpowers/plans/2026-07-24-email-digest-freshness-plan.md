# 定时行情邮件数据新鲜度 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让定时行情邮件的每个分析块显示真实数据时间和过期状态，并修复波动率静默降级、复盘陈旧与重复计样本、Windows 集中补跑问题。

**Architecture:** 在现有分析数据类中附带仅供本次进程绘图使用的短窗口快照，邮件分析与图表复用该快照。统一的时间元数据和新鲜度函数负责 HTML/文本警告；复盘按 24 小时刷新并去重；调度脚本只为 08:00 开启补跑。

**Tech Stack:** Python 3.11、dataclasses、pytest、HTML 邮件、PowerShell Task Scheduler。

## Global Constraints

- 所有显示时间使用北京时间 UTC+8。
- 过期数据仍发送，但必须显示红色警告。
- 过期阈值固定为 1H=2h、4H=6h、1D=36h、1W=8d。
- 不改变现有策略、均线参数和邮件发送时段。
- 只修改任务相关文件，不清理无关代码。

---

### Task 1: 数据时间与过期状态

**Files:**
- Modify: `okx_quant/multi_coin_market_digest.py`
- Modify: `okx_quant/btc_market_analyzer.py`
- Test: `tests/test_multi_coin_market_digest.py`

**Interfaces:**
- Produces: `ChartDataStatus`、`build_chart_data_status(...)`、图表时间元数据映射。
- Consumes: `Candle.ts`、`Candle.confirmed`、邮件 `generated_at`。

- [ ] 写失败测试：正常、过期、进行中 K 线的北京时间与状态文本。
- [ ] 运行目标测试并确认因缺少状态接口而失败。
- [ ] 实现统一时间格式和周期过期阈值。
- [ ] 让分析对象保留最后一段绘图快照，图表不再因缓存数量足够而跳过刷新。
- [ ] 在每个图格、补充分析周期行和邮件顶部渲染数据时间及过期警告。
- [ ] 运行目标测试并确认通过。

### Task 2: 波动率同批数据与显式降级

**Files:**
- Modify: `okx_quant/multi_coin_market_digest.py`
- Test: `tests/test_multi_coin_market_digest.py`

**Interfaces:**
- Produces: `BtcVolatilitySupplement.candle_series`。
- Consumes: Deribit 波动率指数；失败时消费 OKX 价格快照生成程序历史波动率。

- [ ] 写失败测试：邮件准备阶段复用分析阶段波动率序列，不再次请求 Deribit。
- [ ] 写失败测试：降级来源显示“降级：程序历史波动率”和实际数据时间。
- [ ] 运行测试并确认预期失败。
- [ ] 在补充分析中保存短窗口 K 线序列并用于图表。
- [ ] 删除邮件准备阶段的二次波动率采集。
- [ ] 运行目标测试并确认通过。

### Task 3: 复盘自动刷新与样本去重

**Files:**
- Modify: `okx_quant/multi_coin_market_digest.py`
- Modify: `okx_quant/analysis_email_validation.py`
- Test: `tests/test_multi_coin_market_digest.py`
- Test: `tests/test_analysis_email_validation.py`

**Interfaces:**
- Produces: 24 小时刷新判定、`sample_cutoff_at`。
- Consumes: 归档 `report_path`、`generated_at`、验证报告 `generated_at`。

- [ ] 写失败测试：超过 24 小时的复盘触发刷新。
- [ ] 写失败测试：相同报告的重复已发送归档只计一次。
- [ ] 运行测试并确认预期失败。
- [ ] 实现带异常保底的过期刷新和样本截止时间。
- [ ] 按 `report_path + symbol`，缺失时按 `generated_at + symbol` 去重。
- [ ] 在文本与 HTML 中显示复盘生成时间、样本截止时间及过期警告。
- [ ] 运行目标测试并确认通过。

### Task 4: 调度补跑策略与端到端验证

**Files:**
- Modify: `scripts/register_btc_market_analysis_schedule.ps1`
- Modify: `okx_quant/email_schedule_manager.py`
- Test: `tests/test_email_schedule_manager.py`

**Interfaces:**
- Produces: 仅 08:00 任务 `StartWhenAvailable=True`。

- [ ] 写失败测试：注册脚本只为 08:00 配置补跑，事件 114 有可读标签。
- [ ] 运行测试并确认预期失败。
- [ ] 调整每个时段独立的任务设置并补充事件标签。
- [ ] 运行全部相关测试。
- [ ] 重新注册任务并读取任务属性确认补跑设置。
- [ ] 生成不发送的真实邮件预览，核对时间、来源、警告和图文一致性。
