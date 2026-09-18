# qqokx ChatGPT 人工交易助理——AI Snapshot V1.0 冻结开发方案

**版本：V1.0 Frozen**  
**目标项目：qqokx**  
**阶段定位：第一阶段——消除人工截图，让 ChatGPT 网页版直接读取 qqokx 导出的结构化快照文件。**

---

## 1. 项目目标

qqokx 当前已经具备行情、K线、DVOL、人工仓位、期权 Greeks、历史仓位、备注等数据能力。

本次开发**不建立新的交易系统，也不修改现有程序化交易系统**。

本阶段仅新增一个独立功能：

> **一键生成供 ChatGPT 网页版分析的 AI Snapshot JSON 文件。**

最终用户操作流程：

```text
qqokx
  ↓
点击“生成 ChatGPT 分析快照”
  ↓
检查行情和波动率数据的新鲜度及时间一致性
  ↓
读取多周期行情、DVOL、全部人工仓位等数据
  ↓
生成一个 JSON Snapshot
  ↓
本地永久保存
  ↓
用户上传到 ChatGPT 网页版
  ↓
用户直接询问：
“分析一下当前行情和我的人工仓位”
```

第一阶段的核心验收标准：

> 用户不再需要手工截图周线、日线、4H、1H、DVOL、人工仓位等主要数据。

---

## 2. V1 明确不做的内容

V1 不得扩大需求范围。

暂时不做：

- AI 自动下单；
- AI 自动撤单；
- AI 修改现有仓位；
- AI 修改程序化策略；
- ChatGPT API 自动调用；
- 自动每小时调用 AI；
- 重大事件自动触发 AI；
- AI 聊天窗口；
- 组合 K 线导出；
- 新建一套行情下载系统；
- 新建一套均线计算系统；
- 新建独立仓位系统；
- 重构现有程序化交易系统。

第二阶段、第三阶段以后再开发。

---

## 3. 核心开发原则

Codex 必须优先复用 qqokx 已有代码和数据。

AI Snapshot 应尽量读取：

```text
qqokx当前真正使用的行情数据
qqokx当前真正使用的EMA15
qqokx当前真正使用的MA50
qqokx当前DVOL数据
qqokx当前人工仓位数据
qqokx当前Greeks
qqokx当前历史仓位
qqokx当前备注字段
```

禁止为了 Snapshot 重新开发另一套计算逻辑，避免出现：

> qqokx 图表看到的 EMA15/MA50  
> ≠  
> AI Snapshot 中的 EMA15/MA50

AI 获得的数据必须尽可能与用户肉眼看到的 qqokx 图表一致。

---

## 4. 架构必须支持多个标的

虽然现阶段主要使用 BTC，但数据协议和内部代码不得写死 BTC。

建议抽象为：

```text
AssetAnalysisGroup
│
├── BTC
│   ├── perpetual_market
│   ├── volatility
│   └── manual_positions
│
├── ETH
│   ├── perpetual_market
│   ├── volatility
│   └── manual_positions
│
└── future_underlying
```

每个标的独立拥有：

```text
underlying
market_source
perpetual_instrument
volatility_source
market_data
volatility_data
manual_positions
data_quality
```

未来增加 ETH、SOL 等时，不需要重新修改 Snapshot 总体协议。

---

## 5. 技术分析行情统一使用永续合约

每个标的默认使用对应**永续合约**作为技术分析行情源。

BTC 示例：

```text
BTC-USDT-SWAP
```

具体 instrument ID 应优先沿用 qqokx 当前实际使用的名称，不要为了 Snapshot 强制改变现有命名。

---

## 6. K线周期及数量

每个标的永续行情固定导出：

```text
1W × 最近250根
1D × 最近250根
4H × 最近250根
1H × 最近250根
```

每根至少包含：

```text
timestamp
open
high
low
close
volume
ema15
ma50
is_closed
```

如果 qqokx 当前还有有价值的现成字段，可一起导出，不需要刻意删除。

---

## 7. 250根只是“输出数量”，不是指标计算数量

这是硬性要求。

禁止：

```text
只读取250根K线
↓
再从第1根重新开始计算EMA15和MA50
```

必须：

```text
使用qqokx现有的充分历史数据
↓
按照qqokx现有图表算法计算EMA15和MA50
↓
完成足够的指标预热
↓
最后截取最近250根
↓
写入AI Snapshot
```

核心验收：

> Snapshot 中最近K线、EMA15、MA50必须与 qqokx 图表显示值一致。

尤其不能因为 Snapshot 导致 EMA 初始化方式不同。

---

## 8. 当前未完成K线必须保留

当前正在形成的 1H、4H、1D、1W 均允许进入 Snapshot。

但必须明确：

```json
"is_closed": false
```

已经正式收盘的K线：

```json
"is_closed": true
```

AI 不得依靠猜测判断一根K线是否已经收盘。

---

## 9. 波动率数据

对于已经配置波动率数据源的标的，导出：

```text
1D × 250
4H × 250
1H × 250
```

每根至少：

```text
timestamp
open
high
low
close
ema15
ma50
is_closed
```

没有 volume 不需要虚构。

EMA15、MA50同样必须使用足够历史数据预热后，再输出最后250根。

BTC 当前主要对应 DVOL。

---

## 10. 数据新鲜度与时间一致性是硬性要求

用户点击“生成 ChatGPT 分析快照”以后，第一步不是直接导出，而是执行：

> **Freshness & Alignment Check**

必须先确认：

1. 永续K线是最新数据；
2. 波动率是最新数据；
3. 两者时间保持一致；
4. 精度检查到小时级即可。

Snapshot 顶部必须至少保存：

```text
snapshot_time
effective_as_of
timezone
```

建议保存真实生成时间，并同时记录：

```text
effective_as_of_hour
```

例如：

```text
实际生成时间：
2026-09-18 17:38:25

effective_as_of_hour：
2026-09-18 17:00
```

---

## 11. 新鲜度判定

对每一个 AssetAnalysisGroup 独立检查。

例如 BTC：

```text
BTC永续行情
BTC DVOL
```

二者必须都已经更新到当前有效小时。

示例：

```text
生成时间：17:38

BTC行情最新数据：17:xx / 17:00有效小时
BTC DVOL最新数据：17:xx / 17:00有效小时

→ PASS
```

如果：

```text
BTC行情：17:00
BTC DVOL：15:00
```

则：

```text
→ FAIL
```

原则：

> **核心行情和已启用的波动率数据，不允许使用明显过期的数据生成正式 Snapshot。**

允许最大新鲜度误差：

```text
60分钟
```

但核心目标不是简单“差不多60分钟”，而是：

> **行情与波动率属于同一个最新小时级分析时点。**

---

## 12. 高周期当前K线

例如17:38生成Snapshot：

```text
1H当前K可能尚未结束
4H当前K尚未结束
日K尚未结束
周K尚未结束
```

这没有问题。

这些K线应反映截至 Snapshot 当前数据源最新状态，并正确记录：

```text
data_as_of
is_closed
```

AI需要能够判断：

> “当前4H只是盘中状态，而不是4H收盘确认。”

---

## 13. 数据质量信息

Snapshot 顶部增加：

```json
"data_quality": {
  "freshness_check": "PASS",
  "alignment_check": "PASS",
  "max_allowed_lag_minutes": 60,
  "warnings": [],
  "errors": []
}
```

每个标的再记录自己的状态，例如：

```json
"BTC": {
  "market_data_as_of": "...",
  "volatility_data_as_of": "...",
  "freshness": "PASS",
  "alignment": "PASS"
}
```

---

## 14. 硬错误与软错误

### 硬错误

以下情况不得生成“正式AI Snapshot”：

```text
核心永续行情严重过期
已启用波动率数据严重过期
行情与波动率小时级时间明显不一致
关键K线数据读取失败
Snapshot JSON序列化失败
```

程序必须明确告诉用户失败原因。

### 软错误

例如：

```text
某一张期权Vega暂时为空
某仓位某个非核心字段为空
某扩展数据暂时不可用
```

允许继续生成，但必须写入：

```text
warnings
```

不得静默忽略。

---

## 15. 人工仓位范围

Snapshot 导出：

> **所有人工仓位。**

包括：

```text
SPOT
PERPETUAL
OPTION
```

未来其他人工仓位类型允许扩展。

程序化服务器自动交易仓位不属于本阶段 AI 人工交易助理的管理对象。

如果当前 qqokx 数据库人工仓和自动仓不能直接区分，应设计统一字段：

```text
trade_origin = manual
trade_origin = automated
```

AI Snapshot 默认只输出：

```text
trade_origin = manual
```

---

## 16. 人工仓位字段原则

原则：

> **qqokx 当前人工仓位对象已经拥有的业务字段，尽量全部导出。**

不要为了“节省数据量”主动删除已有字段。

同时建立部分稳定标准字段，方便 AI 长期读取。

---

## 17. 期权字段

期权至少包括：

```text
account_alias
instrument_id
underlying
instrument_type = OPTION
option_type
side
strike
expiry
quantity
contract_value
entry_price
mark_price
index_price
position_value
unrealized_pnl
realized_pnl
pnl_percent
iv
delta
gamma
vega
theta
open_time
days_to_expiry
note
```

如果 qqokx 已有其他字段，例如：

```text
bid
ask
OI
volume
margin
break_even
其他Greeks
```

尽量原样保留。

---

## 18. 永续字段

至少：

```text
account_alias
instrument_id
underlying
instrument_type = PERPETUAL
side
quantity
entry_price
mark_price
index_price
leverage
margin_mode
margin
liquidation_price
unrealized_pnl
realized_pnl
pnl_percent
funding_rate
open_time
note
```

qqokx已有额外字段继续保留。

---

## 19. 现货字段

至少：

```text
account_alias
asset
underlying
instrument_type = SPOT
quantity
average_cost
current_price
market_value
unrealized_pnl
realized_pnl
pnl_percent
open_time
note
```

---

## 20. 仓位备注是核心字段

qqokx当前已经存在备注系统，并且：

> **备注随历史仓位永久本地保存。**

本次开发不得改变这个逻辑。

Snapshot只读取：

```text
note
```

AI没有修改、删除或重写备注的能力。

当前持仓和历史人工交易记录都应该尽量携带原始备注。

---

## 21. Account Alias

预留：

```text
account_alias
```

不要暴露真实交易所账户ID给AI。

示例：

```text
main
options_manual
btc_manual
```

即使当前只有一个账户，也建议：

```text
account_alias = "main"
```

未来多账户时不需要重新改协议。

---

## 22. 最近人工交易记录

Snapshot V1 建议附带：

```text
最近7天
最多100条
```

人工交易记录。

包括：

```text
time
account_alias
instrument_id
underlying
action
side
quantity
price
position_before
position_after
realized_pnl
note
```

例如：

```text
OPEN
ADD
REDUCE
CLOSE
ROLL
```

优先复用 qqokx 当前已有交易历史结构，不要为了满足以上命名而破坏原数据库。

可以在 Snapshot Builder 层进行字段映射。

---

## 23. 组合信息

V1 暂时：

> **不导出组合K线。**

组合K线需要时用户继续临时截图。

但如果 qqokx 当前已经有组合汇总 Greeks，应直接导出：

```text
net_delta
net_gamma
net_vega
net_theta
total_unrealized_pnl
total_realized_pnl
```

禁止为了 Snapshot V1 重新写一套独立组合 Greeks 引擎。

有现成值则复用；没有则允许为空。

---

## 24. Snapshot 顶层结构建议

建议整体结构类似：

```json
{
  "schema_version": "1.0",
  "snapshot_id": "...",
  "snapshot_time": "...",
  "effective_as_of_hour": "...",
  "timezone": "Asia/Shanghai",
  "data_quality": {},
  "assets": {
    "BTC": {},
    "ETH": {}
  },
  "manual_positions": [],
  "portfolio_summary": {},
  "recent_manual_trades": []
}
```

具体字段名称允许 Codex 根据现有 qqokx 代码风格优化。

但语义不得改变。

---

## 25. 原始字段保留机制

如果 qqokx 某个业务对象有很多现有字段，而标准协议没有列出，不要直接丢弃。

允许采用：

```json
"raw": {
  "...": "..."
}
```

原则：

> 标准字段保证 AI 稳定读取；原始字段避免信息损失。

---

## 26. Snapshot 文件

统一使用 JSON。

建议文件名：

```text
qqokx_AI_Snapshot_YYYYMMDD_HHMMSS.json
```

例如：

```text
qqokx_AI_Snapshot_20260918_173825.json
```

---

## 27. 本地永久保存

每次生成都在 qqokx 本地留档。

建议目录：

```text
ai_snapshots/
└── 2026/
    └── 09/
        └── 18/
            └── qqokx_AI_Snapshot_20260918_173825.json
```

当前阶段默认长期保存。

V1暂不开发自动删除、云同步、复杂压缩等功能。

---

## 28. qqokx UI

V1保持极简。

增加主要按钮：

> **生成 ChatGPT 分析快照**

运行过程：

```text
检查行情新鲜度
↓
检查波动率新鲜度
↓
检查时间一致性
↓
读取各标的250根数据
↓
读取人工仓位
↓
读取Greeks
↓
读取最近人工交易
↓
生成JSON
↓
保存
```

成功以后显示：

```text
Snapshot生成成功

生成时间
有效分析时间
包含标的
人工仓位数量
数据质量状态
文件完整路径
```

提供：

```text
打开文件所在目录
复制文件路径
打开最新Snapshot目录
```

无需开发复杂AI页面。

---

## 29. 第一阶段AI权限

本阶段 AI 数据接口逻辑必须按：

> **完全只读**

设计。

AI Snapshot 不允许产生任何：

```text
下单
撤单
改单
平仓
修改策略
修改仓位
修改备注
修改数据库
```

功能。

Snapshot Builder 只能：

```text
READ
SERIALIZE
SAVE LOCAL SNAPSHOT
```

---

## 30. 不得影响现有程序化交易

AI Snapshot 功能必须是独立辅助模块。

不得改变：

```text
自动交易策略执行
订单管理
服务器程序化交易
原有风控
策略状态
自动持仓管理
```

可以共用：

```text
行情
数据库
Greeks
缓存
指标
```

但不能把 AI Snapshot 嵌入自动交易核心执行链。

---

## 31. Codex 第一轮任务——禁止直接修改代码

收到本冻结方案以后，第一轮 Codex 必须：

> **先检查现有 qqokx 项目，不得立刻写代码。**

第一轮输出必须包含：

1. 当前 qqokx 中对应行情模块的位置；
2. BTC/其他永续K线当前如何获取和保存；
3. EMA15、MA50当前计算位置和方法；
4. DVOL当前数据来源和存储方式；
5. 人工持仓数据模型；
6. 自动仓位与人工仓位当前如何区分；
7. 期权 Greeks 当前来源；
8. 历史仓位及备注如何保存；
9. 最近人工交易记录当前数据来源；
10. 组合 Greeks 是否已经存在；
11. 可直接复用的模块；
12. 需要新增的最小模块；
13. 建议新增/修改哪些文件；
14. Snapshot Builder完整数据流；
15. 是否存在对现有程序化系统的潜在影响；
16. 当前方案与项目现状之间是否存在冲突。

第一轮只做：

> **源码核对 + 差距分析 + 实施方案**

不要直接开发。

---

## 32. 第二轮实施原则

第一轮确认以后再开发。

建议优先实现一个独立：

```text
AISnapshotBuilder
```

其职责仅：

```text
读取数据
检查新鲜度
检查时间一致性
字段标准化
生成Snapshot对象
数据质量检查
JSON序列化
本地保存
```

不要让 Builder 自己负责：

```text
行情下载
交易执行
重新计算独立指标系统
修改仓位
```

---

## 33. 测试要求——K线一致性

随机选：

```text
BTC 1H
BTC 4H
BTC 1D
```

至少检查若干根：

```text
timestamp
O
H
L
C
EMA15
MA50
is_closed
```

与 qqokx 原图表进行对照。

验收要求：

> 数值来源及计算结果与当前qqokx图表保持一致。

---

## 34. 测试要求——250根指标预热

必须专项检查：

> Snapshot 第1根及其附近 EMA15 / MA50 是否因为只计算250根而产生初始化偏差。

不得出现这种情况。

---

## 35. 测试要求——新鲜度

模拟：

```text
行情最新
DVOL最新
```

应：

```text
PASS
```

模拟：

```text
行情最新
DVOL滞后2小时
```

应：

```text
FAIL
```

并阻止正式快照生成。

---

## 36. 测试要求——当前K线状态

检查：

```text
1H
4H
1D
1W
```

当前未完成K线的：

```text
is_closed
```

必须准确。

---

## 37. 测试要求——仓位

对照 qqokx 当前人工仓位界面：

```text
仓位数量
合约代码
方向
数量
成本
Mark
盈亏
IV
Delta
Gamma
Vega
Theta
备注
```

Snapshot必须一致。

---

## 38. 测试要求——人工与自动仓位隔离

必须测试：

> 自动程序化仓位不能误进入 AI 人工仓位管理 Snapshot。

如果用户未来主动要求加入，再另行扩展。

---

## 39. ChatGPT实战验收

开发完成后实际生成 Snapshot。

用户只上传 JSON 文件给 ChatGPT，不附带普通行情截图，然后询问：

> “分析一下现在行情和我的人工仓位。”

目标：

> ChatGPT能够完成当前依靠大量截图进行的大部分行情、DVOL、仓位管理和机会分析工作。

如果实战中发现缺少重要字段，再升级：

```text
Schema V1.1
```

不得一开始无限扩张 V1。

---

## 40. 为第二阶段预留，但现在不实现

以后第二阶段可能增加：

```text
Signal/Event Library
重大变化检测
人工仓位关联
临时每小时监控
事件触发AI
```

V1设计时不得人为阻碍这些扩展，但不要提前实现。

---

## 41. 为第三阶段预留，但现在不实现

以后可能增加：

```text
仓位状态机
上一次AI判断
连续分析
历史假设
失效条件
AI复盘
新交易机会监控
```

当前保存的：

```text
Snapshot历史
仓位备注
人工历史交易
```

未来会作为这些功能的重要数据基础。

---

## 42. V1 最终原则

整个第一阶段只解决一个核心问题：

> **用户现在需要通过大量截图才能让ChatGPT理解qqokx中的行情、DVOL和人工仓位。V1将这些数据自动形成统一、可靠、时间一致的JSON Snapshot，让用户一次上传即可完成分析。**

不要将第一阶段做成复杂AI系统。

先把：

> **数据准确、一致、最新、完整、方便导出**

做好。

---

# 给 Codex 的启动指令

请把以上《qqokx ChatGPT人工交易助理——AI Snapshot V1.0 冻结开发方案》作为当前唯一需求基准。

**第一轮不要修改任何代码。**

先完整检查现有 qqokx 源码，对照冻结方案逐项核实已有能力、数据来源和可复用模块，输出：

- 现状核对；
- 差距分析；
- 数据流；
- 拟修改文件；
- 实施步骤；
- 风险及冲突。

优先复用现有代码，禁止重复建设行情、指标、仓位和 Greeks 系统。

等用户确认第一轮方案后，再开始编码。
