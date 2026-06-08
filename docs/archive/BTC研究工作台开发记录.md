# BTC 研究工作台开发记录

更新时间：2026-05-06

## 总目标

把 `BTC行情分析`、`行情日记`、K线图、人工标注、程序信号、波动率、后续复盘整合成一个统一的 `BTC研究工作台`。

长期目标不是做普通日记，而是沉淀个人交易研究 skill：

- 人工随笔：主观判断、价格行为形态、关键位、失效条件。
- 程序分析：1H / 4H / 1D 方向、评分、信号、共振变化。
- 图表证据：K线、人工划线、截图、日记锚点。
- 波动环境：波动率状态、波动扩张/压缩、形态在不同波动环境下的表现。
- 复盘结果：未来行情验证、对错/部分对、最大浮盈、最大回撤、方法统计。

## 当前约定

- 暂时不接收费模型。
- 用户先在豆包生成结构化 JSON，再粘贴回本软件。
- 软件必须保留完整 JSON，不擅自脑补字段。
- 如果字段不足，后续 UI 应提示需要补充，而不是自动编造。
- 研究工作台先做分析与复盘，不放开多/开空执行按钮。

## 当前阶段

阶段：样本结构升级（已完成第一步）

目标：让 `journal` 模块支持用户和豆包约定的完整研究样本 JSON，不只支持旧版简化提炼字段。

## 进度清单

- [x] 已有第一版 `journal.py`：本地规则提炼、AI JSON 粘贴解析。
- [x] 已有第一版 `journal_ui.py`：行情日记窗口、保存、AI JSON 粘贴、本地提炼、附件选择。
- [x] 已接入主界面工具菜单：打开行情日记。
- [x] 新增完整研究样本标准解析。
- [x] 新增完整研究样本提示词生成。
- [x] UI 的“生成 AI 提示词”改为完整标准。
- [x] UI 显示 record_type / hypothesis / verification_plan 等关键字段。
- [ ] 后续新增 BTC 研究工作台统一窗口。
- [ ] 后续叠加 K线主图、程序方向变化点、波动率副图。

## 本轮计划

1. 已增加完整研究样本 JSON 的标准解析函数。
2. 已让旧版 `parse_ai_extraction_paste` 能识别完整 JSON，并映射到现有提炼预览字段。
3. 已增加测试，覆盖 `market_view` 完整样本。
4. 已更新 UI 预览：记录类型、核心假设、验证窗口。

## 本轮完成记录

完成时间：2026-05-06

已改动：

- `okx_quant/journal.py`
  - `parse_ai_extraction_paste` 支持完整研究样本 JSON。
  - `build_ai_extraction_prompt` 改为输出完整研究样本提示词。
  - 完整样本会保留在 `raw_payload`，同时映射出旧预览字段。
- `okx_quant/journal_ui.py`
  - 提炼结果区新增记录类型、核心假设、验证窗口预览。
- `tests/test_journal.py`
  - 新增完整 `market_view` 样本导入测试。
  - 新增完整提示词结构测试。

验证：

- `python -m py_compile okx_quant\journal.py okx_quant\journal_ui.py okx_quant\persistence.py okx_quant\ui_shell.py`
- `python -m unittest tests.test_journal tests.test_persistence`

下一步建议：

1. 把完整研究样本字段从 `raw_payload` 正式提升成 `JournalResearchSample` dataclass。
2. 在 UI 里增加完整 JSON 查看/复制区，便于人工核对。
3. 开始设计 `BTC研究工作台` 新窗口骨架：左侧样本列表，中间 K线，右侧日记/程序分析/复盘。

## 中断恢复提示

恢复后先看：

- `git status --short`
- `BTC研究工作台开发记录.md`
- `okx_quant/journal.py`
- `tests/test_journal.py`

当前已知无关工作树变化：

- `scripts/123.bat` 显示为删除，暂时不要处理，除非用户明确要求。

# 2026-05-06 BTC研究工作台叠加/拖动/复盘锚点记录

本次继续内容：
- `okx_quant/btc_research_workbench_ui.py`
  - 重构了工作台 K 线渲染底座，主图、波动率图、叠加对比图统一走同一套 `Canvas` K 线绘制逻辑。
  - `叠加对比` 不再依赖价格和波动率时间戳完全相等才显示；改为按 `1H / 4H / 1D` 时间桶对齐，避免日线场景出现空白。
  - 增加图表视口 `ChartViewport`，支持滚轮缩放、拖动画布平移、双击重置视图。
  - 增加人工画图持久化：趋势线 / 水平线 / 矩形 / 平行通道会按 `BTC-USDT-SWAP|timeframe` 存到状态文件，下次打开继续保留。
  - 增加历史分析锚点读取：从 `reports/analysis/*.json` 提取已有 BTC 分析结果，在主图上显示历史分析方向/评分锚点，作为复盘入口。
  - 重绘逻辑改为调度式刷新，减少画图拖动过程中的闪动和空白时间。
- `okx_quant/persistence.py`
  - 新增 `btc_research_workbench_state.json` 的路径、读取、保存函数，用于保存工作台画图和视口状态。
- `okx_quant/ui_shell.py`
  - 工作台窗口打开时继续复用主程序 `OkxRestClient`，并额外传入 `DeribitRestClient`，让波动率优先读取系统已有 Deribit 数据。
- 测试补充：
  - `tests/test_persistence.py` 新增 BTC 研究工作台状态存取测试。
  - `tests/test_btc_research_workbench_ui.py` 新增三类工作台辅助逻辑测试：
    - 日线叠加对齐
    - 历史波动率生成
    - 历史分析锚点读取

验证：
- `python -m py_compile okx_quant\btc_research_workbench_ui.py okx_quant\ui_shell.py okx_quant\persistence.py tests\test_persistence.py tests\test_btc_research_workbench_ui.py`
- `python -m unittest tests.test_journal tests.test_persistence tests.test_btc_research_workbench_ui`

当前结果：
- 26 个测试通过。

下一步：
1. 在主图叠加更完整的历史分析复盘信息，比如悬停提示、报告时间、理由摘要。
2. 把已有 BTC 行情分析窗口里的“立即分析”能力进一步并入工作台，而不只是读取历史报告锚点。
3. 继续优化图表交互体验，补十字光标、当前值浮层、时间轴拖动反馈。
# 2026-05-06 波动率页签与画图工具记录

本次继续内容：

- `BTC研究工作台` 中间区已改为参考期权策略图大窗的多页签结构：
  - `BTC主图`
  - `波动率K线`
  - `叠加对比`
- 主图页已加入轻量人工工具：
  - 趋势线
  - 水平线
  - 矩形
  - 平行通道
  - 撤销一笔 / 清空画图
- 波动率数据来源升级为三层回退：
  - 优先读取系统已有的 Deribit 波动率缓存
  - 缓存没有时尝试直接请求 Deribit 波动率指数
  - 仍不可用时退回程序历史波动率
- 叠加对比页已采用上下双图布局：
  - 上图价格K线
  - 下图波动率K线

验证：

- `python -m py_compile okx_quant\btc_research_workbench_ui.py okx_quant\ui_shell.py` 通过。
- `python -m unittest tests.test_journal tests.test_persistence` 通过，当前 21 个测试。

下一步：

1. 把人工画图持久化，和 journal / 样本时间点绑定。
2. 在主图叠加 1H / 4H 程序方向改变点。
3. 给波动率页和叠加页补 hover / 十字光标 / 当前值浮层。

# 2026-05-06 K线接入记录

本次继续内容：

- `BTC研究工作台` 已复用主程序 `OkxRestClient`。
- 工作台打开后会尝试加载 `BTC-USDT-SWAP` K线。
- 支持在工作台顶部选择 `1H / 4H / 1D` 并重新加载。
- 中间 Canvas 已从占位图升级为真实蜡烛图绘制。
- 程序分析/波动率区域已先显示：
  - 最新收盘价
  - 上一根涨跌幅
  - 近 30 根平均振幅
- 后续仍需继续接：
  - 人工划线与标记
  - 日记锚点
  - 1H / 4H 程序方向改变点
  - ATR/波动率副图

验证：

- `python -m py_compile okx_quant\journal.py okx_quant\journal_ui.py okx_quant\btc_research_workbench_ui.py okx_quant\persistence.py okx_quant\ui_shell.py` 通过。
- `python -m unittest tests.test_journal tests.test_persistence` 通过，当前 21 个测试。

# 2026-05-06 工作台骨架记录

本次继续内容：

- 新增 `okx_quant/btc_research_workbench_ui.py`。
- 第一版 `BTC研究工作台` 三栏骨架已完成：
  - 左侧：研究样本/日记列表，从现有 journal 持久化数据读取。
  - 中间：K线主图骨架占位，预留人工划线、标记、程序方向改变点。
  - 中下：程序分析与波动率叠加占位。
  - 右侧：原始随笔 + 结构化 JSON 查看。
- 已接入主界面工具菜单：`打开BTC研究工作台`。
- 已在关闭主程序时销毁工作台窗口。

验证：

- `python -m py_compile okx_quant\journal.py okx_quant\journal_ui.py okx_quant\btc_research_workbench_ui.py okx_quant\persistence.py okx_quant\ui_shell.py` 通过。
- `python -m unittest tests.test_journal tests.test_persistence` 通过，当前 21 个测试。

下一步：

1. 工作台中间图表接真实 BTC K线数据。
2. 在 K线上显示人工标记/截图锚点。
3. 叠加 1H / 4H 程序方向改变点。
4. 增加 ATR/波动率副图和状态标签。

# 2026-05-06 UI 增强记录

本次继续内容：

- `okx_quant/journal_ui.py` 已增加完整结构化 JSON 预览区。
- 导入豆包/AI JSON 后，预览区会显示软件最终保存的标准 JSON。
- 已增加“复制完整 JSON”按钮，便于和豆包输出继续对齐字段。

验证：

- `python -m py_compile okx_quant\journal.py okx_quant\journal_ui.py okx_quant\persistence.py okx_quant\ui_shell.py` 通过。
- `python -m unittest tests.test_journal tests.test_persistence` 通过，当前 21 个测试。

下一步：

1. 开始搭 `BTC研究工作台` 统一窗口骨架。
2. 第一版先做布局，不接真实 K 线绘图逻辑：左侧样本/日记，中间图表占位，右侧结构化分析与复盘。

# 2026-05-06 继续开发记录

本次继续内容：

- 已先验证断点状态：
  - `python -m py_compile okx_quant\journal.py okx_quant\journal_ui.py okx_quant\persistence.py okx_quant\ui_shell.py`
  - `python -m unittest tests.test_journal tests.test_persistence`
- 已新增完整研究样本数据结构：
  - `JournalCondition`
  - `JournalHypothesis`
  - `JournalExecutionPlan`
  - `JournalObservation`
  - `JournalVerificationPlan`
  - `JournalResearchSample`
- 已新增 `parse_research_sample_paste`，用于把豆包/AI 输出的完整 JSON 直接转成结构化样本。
- 已让完整研究样本导入继续兼容旧的 `JournalExtractionResult` 预览逻辑。
- 已新增结构化样本 round-trip 测试和完整样本粘贴解析测试。

当前验证结果：

- `python -m py_compile okx_quant\journal.py okx_quant\journal_ui.py okx_quant\persistence.py okx_quant\ui_shell.py` 通过。
- `python -m unittest tests.test_journal tests.test_persistence` 通过，当前 21 个测试。

下一步：

1. 在日记 UI 中增加“完整结构化 JSON”查看/复制区域。
2. 再开始搭 `BTC研究工作台` 统一窗口骨架。
#
# 2026-05-07 图表交互对齐记录

本次继续内容：
- 继续把 `BTC研究工作台` 的 K 线交互向 `okx_quant/backtest_ui.py` 对齐。
- 修正共享视口被副页签重绘回写的问题：
  - 主价格图使用 `persist=True`
  - 波动率页与叠加页只跟随当前视口，不再改写主视口
- 继续收重绘路径：
  - `<<NotebookTabChanged>>` 仍然触发重绘
  - 但真正执行时只重绘当前激活页签
  - 主图、波动率页、叠加页不再每次一起重刷

目的：

1. 让主图的 X 轴和拖动窗口只由主价格序列决定。
2. 降低隐藏页签参与重绘带来的时间窗抖动和体验噪音。
3. 继续把工作台图表行为向系统里最成熟的回测图实现靠拢。

# 2026-05-07 行情日记融合记录

本次继续内容：
- 把行情日记的核心工作流直接并入 `BTC研究工作台` 右侧面板。
- 右侧不再只是原始随笔 + JSON 查看，而是补齐：
  - 新建日记
  - 保存日记
  - 本地提炼
  - 复制 AI 提示词
  - 导入 AI JSON
  - 添加截图附件
  - 复制结构化 JSON
- 新增提炼结果预览区：
  - 状态
  - 来源
  - 标的
  - 周期
  - 方向
  - 动作
  - 验证窗口
  - 摘要
- 研究样本列表和右侧编辑区现在共用同一套 journal 持久化数据。

当前结果：

1. 可以在 `BTC研究工作台` 内直接写行情日记。
2. 可以把豆包/AI 输出的 JSON 直接粘进工作台导入。
3. 可以在同一界面里保存、查看、复用附件和结构化结果。
