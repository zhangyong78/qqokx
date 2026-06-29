# OKX 策略工作台

当前版本：`v0.6.51`

一个面向 OKX 的桌面量化交易工作台，围绕策略运行、交易辅助、回测研究和分析导出构建，适合做策略验证、实盘辅助和研究沉淀。

## 协作快捷口令

为减少重复沟通，当前仓库约定一个固定口令：

- 当你说 `123` 时，表示：让我同步更新 `README.md` 和 `软件开发指南.md`
- 默认理解为：根据当前这轮已经完成的功能、结构或流程改动，补齐这两份文档
- 如果某次你只想更新其中一份文档，需要单独明确说明

这条约定从现在开始生效，后续我会按这个口令执行。

## 项目概览

当前仓库主要包含以下能力：

- 多策略会话运行与恢复接管
- 持仓、历史成交、历史仓位查看
- Smart Order、条件单、网格类交易辅助
- 现货套利：机会扫描、套利开仓/平仓、持仓配对平仓、套利图表
- 期权仓位保护、期权策略计算、展期建议
- Deribit 波动率查看与监控
- 回测、参数矩阵对比、结果持久化
- BTC 研究工作台与报告导出

## 近期更新

`v0.6.51` 这一轮版本内容比较集中，重点新增和调整如下：

- API 环境与策略环境的一致性防呆补齐：
  - 启动确认弹窗现在会同时显示 `API环境`、`策略环境` 和 `环境状态`
  - 当 API 配置保存的是 `demo/live`，但策略准备在另一套环境下单时，会明确显示 `不一致（将阻止启动）`
  - 从策略模板、组合包、会话记录回填到启动区时，会优先跟随目标 API 配置的环境，不再盲目沿用导出时的旧环境字段
  - 非 `signal_only` 的真实下单会话在启动前会做环境校验，避免把模拟盘 Key 带去实盘，或把实盘 Key 带去模拟盘
  - 如果 OKX 返回 `50101` 或 `APIKey does not match current environment`，错误提示现在会直接引导检查 API 配置保存的是 `demo` 还是 `live`
- 动态限价单的 OKX 止损托管补了一层失败兜底：
  - 当 `动态止盈 / 初始不挂止盈` 场景下，OKX 因附带止损参数拒绝主挂单时
  - 现在会自动改为“先挂裸限价单，成交后再补独立止损算法单”
  - 可以减少因为 `51051` 一类止损价格校验问题，导致整笔动态限价单直接错过的情况
  - 成交后补挂的独立止损单也会继续接入原有的动态止损上移监控
- 策略模板/快照的动态保护规则序列化补齐：
  - `dynamic_protection_rules` 现在会按标准 payload 写入配置快照
  - 导出模板、保存快照、后续导入回填时，更不容易丢失规则化动态保护细节
- 新增 `EMA55 斜率做空` 策略：
  - 规则：`EMA55` 单根斜率比例小于等于阈值时开空，斜率重新转正时平仓
  - 支持 launcher / backtest 双端参数
  - 斜率阈值可调，默认值为 `-0.0005`
- 动态保护口径开始从 `2R 保本` 统一升级为 `nR 保本`：
  - 支持按策略配置单独指定 `首档触发R`
  - 当 `首档触发R = 2` 时，仍保留“先抬到保本位”的特例
  - 启动确认、参数摘要、回测说明、导出报告文案已统一改成 `nR 保本`
- `EMA55 斜率做空` 的动态保护与离场参数继续补齐并正式贯通：
  - 启动区、会话恢复、回测区、导出报告现在共用同一套配置
  - 新增 `首档触发R`，不再只固定按 `2R` 起跳
  - `nR 保本` 会根据 `首档触发R` 判断是否先移到保本位
  - 现在可以显式开启/关闭 `斜率转正平仓`
  - 上述参数都会进入启动确认、参数摘要、回测说明和导出报告
- `EMA55 斜率做空` 的回测与研究能力补齐：
  - 支持固定风险金、固定数量、风险百分比三类仓位口径
  - 支持 `nR 保本`、手续费偏移、时间保本、动态止盈
  - 输出了独立 HTML 研究报告与多组研究脚本
- `EMA55 斜率做空` 的通用版本默认值更新：
  - 固定使用 `EMA55`
  - 默认启用 `斜率转正平仓`
  - 默认 `首档触发R = 5`
- `BTC EMA55 斜率做空` 的研究口径同步调整：
  - 默认改为 `ATR14 + 2ATR 止损`
  - 默认 `5R` 触发 `nR 保本`
  - 描述文案从“2R 保本”切到“可选 nR 保本”
- 多币种默认模板更新了一轮：
  - 做多默认模板改成 `BTC / ETH / SOL / DOGE`
  - 原 `BNB` 做多模板移出
  - 多空模板都开始记录各币种独立的 `首档触发R`
- 持仓大窗刷新体验优化：
  - 打开和刷新时优先更新持仓主视图与当前页签，不再一次性同步所有历史页
  - 下方各标签页改为切换到该页时再刷新
  - 持仓主数据和盈亏折算/合约信息/盘口价格改成分阶段补全，减少主界面阻塞感
- 持仓大窗与手动平仓体验继续提速：
  - 复用持仓大窗时，会先显示主视图，再延后刷新当前活动页签，减少首屏等待
  - “平仓选中”改成后台异步提交，避免主线程被下单与回查阻塞
  - 平仓提交后只做轻量跟进刷新：市价单优先补刷持仓，买一/卖一平仓优先补刷委托，再延后刷新持仓
  - 平仓前会优先复用本地合约缓存，减少重复 `get_instrument` 带来的额外等待
- 运行中策略的“停止”流程改成快停 + 后台审计：
  - 点击“停止选中策略”后，会先立即把会话切到 `已停止`，不再长时间挂在 `停止中`
  - 后台仍会继续检查该策略相关委托、保护单和仓位，并在完成后补日志与风险提醒
  - 停止清理现在只按当前策略对应的 `instType` 拉取委托/历史，不再全市场类型一起扫描
  - 算法单扫描改成按需启用：只有依赖交易所托管止损的策略，才会额外检查保护算法单
  - “清空已停止”会等待后台审计结束后才允许移除，避免会话过早消失但残留风险尚未提示
- “停止中”界面的体感延迟又做了一轮深挖修正：
  - 根因不是单纯 OKX 接口慢，而是主界面的状态刷新循环会在后台审计期间反复把会话刷回 `停止中`
  - 现在只要策略线程已经停下，界面会持续显示 `已停止`，后台审计继续跑，但不再把用户感知重新拖回“还没停完”
  - 这样保留了残留委托/持仓检查的安全性，也避免把“后台审计中”误显示成“前台还在停止中”
- “停止结果”弹窗这轮也做了前后解耦：
  - 根因不是弹窗控件本身慢，而是弹窗之前同步触发了持仓/委托重刷新
  - 现在会先弹出“停止结果 / 停止提醒”，再异步补刷当前账户的持仓和委托视图
  - 这样不影响后台审计与安全检查，只是把提示反馈从重刷新链路里先释放出来
- BTC 分析邮件这轮补了一个“邮件任务管理器”：
  - 可以直接查看 `QQOKX BTC Analysis Email 0000/0400/0800/1200/1600/2000` 六个计划任务的状态、下次运行时间、最近结果和电池/补跑设置
  - 可以查看 Windows 任务计划历史事件，快速判断是任务未触发、脚本失败，还是邮件只是进入了 `archive_only / release_pending_and_send`
  - 可以查看 `email_archives` 里的邮件归档记录，并直接打开 HTML、JSON 和关联分析报告
  - 支持手动运行选中的邮件任务，方便排查“定时没发 / 结果不对 / 归档和投递不一致”
- 现货套利这轮开始拆出“极速版”入口：
  - `main.py --app arbitrage-fast` 现在可以直接启动独立的现货套利极速版进程
  - 主工作台工具菜单也新增了“打开现货套利极速版”，不再只能在原工作台里共用同一个主进程窗口
  - 极速版当前聚焦“交割移仓”场景，默认弱化全市场自动扫描，把界面和刷新资源更集中给当前套利腿与目标合约盘口
  - `ArbitrageWindow(fast_mode=True)` 已切到更轻的界面布局和交互路径，方向上更接近临战交易终端，而不是泛扫描工具页
- 交割移仓这轮又往前走了一步，新增 `roll_terminal_qt/` 原型：
  - 这是独立于 Tk 工作台之外的一套 Qt 专业移仓终端，目标是进一步摆脱 Tk 版在盘口刷新、线程阻塞、切换合约时的卡顿感
  - 当前已拆出 `market/account/order/execution/instrument/opportunity` 多个 service 线程，UI 只负责展示与下发参数，不再自己背太多后台逻辑
  - 终端会自动读取现有 API profile，支持带切换密码的 profile 解锁后再启动私有持仓/订单线程
  - 已接入手动移仓参数、执行方式、自动移仓阈值与执行状态表，方向上更像“专业移仓终端”而不是工作台里的附属页
- Qt 专业套利终端这轮继续补齐“套利开仓”联调口径：
  - 开仓数量现在支持三种输入单位：`按币数`、`按金额(U)`、`按合约张数`
  - 右侧已接入常驻“开仓预估”，会联动显示参考现货价、预估现货买入量、预估合约卖出张数、名义金额和拆单结果
  - 对 `BTC-USD` 这类币本位交割合约，张数和现货币数的换算改成按当前价格折算，不再把 `1 张` 误当成固定 `ctVal` 个币
  - 拆单说明也跟着升级：当输入单位不是“张”时，界面会明确提示“先按当前价格换算成合约张数，再按张数拆单”
- 动态做多这轮补了“趋势线斜率过滤开关”：
  - 回测区现在把“是否启用趋势线斜率过滤”拆成独立开关，不再只能靠阈值字段隐式表达
  - 四个做多默认模板已经按币种分化：`BTC / SOL` 当前默认关闭，`ETH / DOGE` 当前默认开启
  - 最佳参数组合包说明页也新增了“斜率过滤开 / 关结论”对比区，直接展示四个币的收益 / 回撤差异和当前默认结论
- 信号复盘实验室这轮升级了趋势层展示：
  - 图表主趋势线从 `EMA21 / EMA55` 切到 `EMA50 / EMA60 / EMA70`
  - 顶部新增趋势状态色带，用来区分 `多头 / 空头 / 无序`
  - 整体更偏“趋势结构复盘”而不是只盯一快一慢两根均线
- 套利执行这轮补了几层下单稳健性兜底：
  - 现货腿如果 `post_only` 不被 OKX 接受，现在会自动降级成普通 `limit` 再提交，避免现货挂单腿直接卡死
  - 衍生品挂单如果命中 OKX 限价带错误，会自动按交易所返回的买卖价格边界重试
  - 下单前也会先检查最大可开张数，超过当前可开容量时直接拦截，并明确提示“最多还能开多少张 / 本次请求多少张”
- Qt 专业套利终端这轮补了“衍生品持仓对应现货”展示：
  - 持仓表新增“对应现货”列
  - 选中某条交割/永续持仓时，右侧摘要会直接显示对应现货可用余额和总余额
  - 这样做移仓或专业平仓时，不用再自己去脑补这条腿后面大概还剩多少现货
- 回测区新增“纯本地回测”链路：
  - 新增 `纯本地回测（不补拉）` 开关，只使用本地缓存，不再临时联网补拉 K 线
  - 新增本地数据状态提示，会显示当前标的/周期的本地缓存根数和覆盖时间范围
  - 支持先单独同步 `1H / 4H` 等选定周期，不必每次全量同步所有周期
  - 新增“同步价格精度/下单规则”，离线回测前可先缓存合约元数据
- 动态保护参数继续拆细为四段：
  - `保本触发R`
  - `移动止盈触发R`
  - `首档锁盈R`
  - `移动步长R`
  - 上述参数已贯通到启动区、会话编辑、回测区、实盘引擎、回测报告和导出说明
- 动态保护规则继续升级为“规则列表”：
  - `StrategyConfig` 新增 `dynamic_protection_rules`
  - 现在不只支持一套固定四段参数，还支持按 `trigger_r + action + lock_r + trail_mode` 配多条规则
  - 规则支持 `break_even` / `lock_profit` 两类动作
  - 规则支持 `step` 递进，可配置“每隔几 R 再加几 R”
  - 旧的四段参数仍然保留，并会自动转换成兼容规则
- 动态委托做多补了一条新的趋势离场规则：
  - 新增 `trend_ema_close_exit_after_trigger_r_enabled` 和 `trend_ema_close_exit_after_trigger_r`
  - 做多仓位在至少达到指定 `nR` 后，如果后续收盘价跌回趋势 `EMA` 下方，可按收盘价直接离场
  - 这条规则已贯通到回测结果、参数摘要、回测说明与导出文案
- 动态止盈逻辑更细：
  - 非 BTC 专用斜率做空策略现在支持“先保本，再进入移动止盈”
  - `首档锁盈R = 0` 时，按“移动止盈触发R - 移动步长R”自动推导
  - `移动步长R` 不再固定为 `1R`，可按策略独立设置
- 分币种默认模板这一轮继续重定稿：
  - 动态委托做多默认模板改成更明确的 `BTC / ETH / SOL / DOGE` 四币独立参数集
  - BTC 做多默认每趋势开仓次数从 `3` 回调到 `1`，并补了第 `11R` 锁 `10R` 的尾段规则
  - ETH / SOL / DOGE 做多的 `ATR`、挂单参考线、`保本触发R`、`max_entries_per_trend` 和规则列表都已按各自最终模板拆开
  - ETH / SOL / DOGE 斜率做空默认线型也分别固化为 `MA61`、`MA20`、`MA21`，不再统一沿用旧版 `EMA55`
- `BTC EMA55 斜率做空` 入场条件更严格：
  - 连续负斜率开空前，窗口前一根斜率不能已经处于负值延续
  - 更偏向只在新的转弱段开始时开空，减少连续阴跌中的重复追空
- 回测数据读取补了一层“确认K线不够就继续补拉”的兜底：
  - 无论联网回测还是纯本地回测，都会尽量补足目标根数
  - 避免末尾未确认 K 线被过滤后直接少样本
- 回测快照与编号口径重新整理：
  - 当前界面更明确区分 `运行编号` 和 `归档编号`
  - 比较区、详情区、图表区和已保存快照区都改成统一编号展示
  - `Rxxx` 更偏当前会话，`Sxxx` 更偏归档快照，减少“同一结果两个编号”的歧义
- 回测 K 线图主窗继续改版：
  - 图表大窗标题与说明文案改为更明确的“回测K线图”
  - 热力图 / 矩阵图的视口和滚动区域同步更稳定
  - 动态保护参数、快照编号、图表标题之间的联动说明更完整
- 回测审计导出改成流式写出：
  - `capital audit` 与 `operation audit` 不再先整表堆内存
  - 导出时按行迭代写 CSV，适合更大的回测结果
- BTC 动态委托做多默认模板回调：
  - BTC 做多默认组合从更激进口径回调为 `EMA21 / EMA55 / 入场 EMA55 / 2R`
  - 每波最多开仓次数提高到 `3`
- 回测前置校验更完整：
  - 纯本地模式下会检查主回测 K 线、多周期过滤 K 线、日线过滤 K 线、高周期方向过滤 K 线是否缺失或有缺口
  - 本地缓存不足时直接提示首段缺口或覆盖范围，不再静默回退联网
- 本地 K 线缓存补了一层“历史未确认自动转确认”：
  - 读取缓存前会把已经完整走完周期、但仍标记为 `confirmed=0` 的旧 K 线自动转正
  - 纯本地回测不再因为早期同步留下的“过期未确认 K 线”而误判样本不足
- 合约元数据缓存正式落地：
  - 新增 `instrument_metadata_cache.json`
  - `get_instrument` / `get_instruments` 支持优先读取本地缓存
  - 网络拉取成功后会自动回写缓存，网络失败时可回退读本地缓存
- 持仓展示在缺少合约规格时补了兜底显示：
  - 对 `OPTION / SWAP / FUTURES`，若拿不到合约规格，先按“张”显示原始合约数量，避免数量口径直接失真
- 持仓估值与规格兜底继续增强：
  - 对常见 `OPTION / SWAP / FUTURES` 增加了合约面值 fallback
  - 期权优先尝试 `uly` / `instFamily` 两套查询口径
  - ticker 缺 bid/ask 时，会再补查一次 order book 顶档价格
- 策略接入结构做了 B 方案重构：
  - 新增 `strategy_ui_schema.py`，集中声明策略 UI 默认值、显示隐藏、强制只读/强制行为
  - 新增 `strategy_runtime_registry.py`，集中声明策略 family、运行入口、方向偏好、参考线标题等
  - launcher / backtest / engine / router 开始共用 schema / registry，不再靠大量 `if strategy_id == ...`
- 修复了新策略接入引发的启动崩溃问题，并补充了启动烟测、策略切换回归和运行路由测试
- 回测说明、参数矩阵、标题文案、方向偏好等逻辑开始按 runtime family 统一分流，减少主程序与具体策略的强耦合
- 启动区与回测区补充了轻量风险金参考提示：
  - 现在不再主打“最小下单门槛动态估算”，而是直接显示基于历史回测整理的 `回测参考`
  - 当前先覆盖 `EMA55 斜率做空` 与 `EMA 动态委托做多`
  - 文案口径统一为 `建议 XXU，最佳 YYU`；其中 `建议` 偏实用，`最佳` 偏更稳覆盖
- 新增一批 BTC 研究脚本与报告产物：
  - `S089` 日线 EMA 对比
  - `S096/S097` 距离确认、最小距离窗口、远距入场提前保护、亏损形态、保护后再入场
  - 对应结果已在 `reports/` 下沉淀为 `latest` 报告与时间戳产物
- 新增最佳参数组合与策略实验脚本：
  - 最佳参数组合包改成 `4 多 + 4 空`
  - 新增 `best_long_trigger_r_experiment`，对比多头策略不同 `首档触发R`
  - 新增 `best_short_line_experiment`，对比空头策略不同均线口径
  - 新增 `best_parameter_bundle_overall`，汇总整组组合包的总体表现
  - 新增 `S140-S160` 策略分析报告脚本
- 新增一批 BTC 多头复盘脚本：
  - `btc_long_5_software_results_analysis`
  - `btc_long_three_config_full_compare`
  - `r001_r003_local_full_compare`
- 新增 `btc_dynamic_long_ma50_vs_ema55_matrix`：
  - 用来对比 BTC 动态委托做多里 `MA50` vs `EMA55` 作为趋势/入场参考线的矩阵结果
- 最佳参数组合包说明文档扩写：
  - 新增策略设计思路
  - 新增动态保护 R 口径说明
  - 新增完整参数 JSON 展示
  - 新增文档末尾更新日志
- 最佳参数组合包说明继续升级：
  - 新增字段级说明
  - 新增动态保护规则口径解释
  - 组合包文档更适合作为长期参数手册回看
- 最佳参数组合包导入体验补了一步“逐条风险金可调”：
  - 导入策略组合弹窗现在会为每条策略显示单独的 `Risk`
  - 默认带出组合包里原始 `risk_amount`
  - 导入前可以按策略分别改风险金，不需要整包统一一个数
  - 自动启动和“只回填首条到启动区”两条导入路径都会优先使用这次输入的风险金
  - 如果某条策略手动填了风险金，会覆盖原固定数量映射，回到风险金模式
- 最佳参数组合包这轮还补了“分层实盘试跑风险金”口径：
  - 组合包内部继续保留统一 `100U` 回测统计口径
  - 但导入实盘试跑时，已经内置 `BTC 20/10`、`ETH 12/8`、`SOL 4/6`、`DOGE 4/6` 的多空分层风险金映射
  - 文档说明里也同步加入了“初期为什么不要所有币一把梭同额度”的解释
- `SOL / DOGE` 动态委托做多模板又做了一轮小定稿：
  - `SOL` 做多从旧的 `7R 锁 1R` 收回到 `5R 锁 1R`
  - `DOGE` 做多主候选从先前误推的 `S652` 更正回 `S653`
  - `DOGE` 做多默认 `保本触发R` 也回到 `2R`
- 多币种市场早报邮件开始带“观点 + 最近复盘”：
  - 邮件正文和 HTML 现在除了强弱排序，还会给出每个币的明确观点
  - 还会附上最近多封邮件的本地回放命中率、最高/最低命中币种、以及“若只做一笔”的提示
  - 早报图表对 `1H / 4H / 1D` 会优先复用本地 K 线缓存，只对 `1W` 直接拉取
- 新增一条“邮件观点有效性验证”链路：
  - 新增 `okx_quant/analysis_email_validation.py`
  - 支持把历史多币种早报邮件按 `4 / 12 / 24 / 72h` 窗口做回放验证
  - 会落地 `JSON / CSV / Markdown` 三种验证结果，方便回看“哪些邮件观点真的有效”
- 五币日线过滤操作包继续做成“配置即文档”：
  - `build_five_coin_daily_filter_operation_pack.py` 现在会根据真实 `StrategyConfig` 自动生成小时摘要、日线摘要和动态保护说明
  - 操作手册和 metadata 不再依赖手写静态文案，降低 bundle 参数变了但手册忘同步的风险
- 实盘轮询链路补了三项轻量扩容优化：
  - 默认轮询间隔从 `3s` 调整为 `10s`
  - 新增 `market_data_hub`，同进程内相同 `instId + bar` 的 K 线共享读取
  - 触发价优先走 `公共 WS ticker`，订单状态优先走 `私有 WS orders`，失败时自动回退 `REST`

主应用代码位于 [okx_quant](/D:/qqokx/okx_quant)，研究与统计相关代码位于 [research](/D:/qqokx/research)、[stats](/D:/qqokx/stats)、[export](/D:/qqokx/export)。

## 目录结构

```text
qqokx/
├─ main.py                  # 桌面应用入口
├─ okx_quant/               # 主应用、交易引擎、回测、UI
├─ research/                # 研究流水线与分析素材
├─ stats/                   # 统计与指标计算
├─ export/                  # 报告与 CSV/HTML 导出
├─ scripts/                 # 打包、诊断、研究与批量执行脚本
├─ tests/                   # 自动化测试
└─ dist/                    # 打包输出
```

## 环境要求

- Python `3.11+`
- `numpy`、`pandas`
- Windows 桌面环境优先

依赖以 [pyproject.toml](/D:/qqokx/pyproject.toml) 为准，推荐使用虚拟环境安装：

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .
```

如果希望按 `requirements.txt` 安装，也可以执行：

```powershell
pip install -r requirements.txt
```

## 启动方式

默认启动：

```powershell
python main.py
```

指定共享数据目录启动：

```powershell
python main.py --data-dir D:\qqokx_data
```

也可以通过环境变量指定数据目录：

```powershell
$env:QQOKX_DATA_DIR = "D:\qqokx_data"
python main.py
```

应用入口位于 [main.py](/D:/qqokx/main.py)，数据目录逻辑位于 [okx_quant/app_paths.py](/D:/qqokx/okx_quant/app_paths.py)。

## 实盘运行建议

当前策略引擎仍然是“多会话 + 定时轮询”的架构，但已经补上了共享行情与 WS 优先的轻量优化，适合中低频实盘。

- 默认轮询：新建策略和保护任务默认 `poll_seconds=10`
- K 线读取：相同 `instId + bar` 在同一进程内共享
- 触发价读取：优先 `公共 WS ticker`，失败时回退 `REST`
- 订单状态：优先 `私有 WS orders`，失败时回退 `REST`
- 持仓 / 账户概览：优先 `私有 WS positions/account`，失败时回退 `REST`

对 `2核 2G` 服务器的建议边界：

- `2 套 API + 1H + 20` 个左右 session：可作为推荐起步规模
- `5 套 API + 5 币种 + 1H + 多空`：可以尝试，但建议先灰度放量
- 再叠加一整套独立 `4H` session：不建议直接在 `2核 2G` 上长期运行

如果是 `1H` 主策略带 `4H` 过滤，而不是再额外开一套完整 `4H` 会话，资源压力会更小一些。

## 数据目录

运行期数据默认放在代码目录同级的 `qqokx_data/` 中，便于代码升级时保留配置与历史状态。

典型结构如下：

- `config/`：API 凭证、用户设置、通知配置
- `cache/`：K 线缓存、Deribit 波动率缓存
- `state/`：策略状态、回测历史、Smart Order 任务、可恢复会话注册表
- `logs/`：运行日志与策略日志
- `reports/`：研究导出、分析报告、实盘导出

兼容迁移规则：

- 首次启动会尝试迁移旧版 `.okx_quant_*` 文件以及原有 `logs/`、`reports/`
- 升级时建议只替换代码目录，保留原 `qqokx_data/`

升级恢复说明：

- 程序关闭或升级重启后，空闲中的策略会话会优先按可恢复注册表自动拉起
- `等待信号` / `signal_only` 这类未持仓的监听任务，重启后会继续恢复监听
- 已有持仓或未完成挂单的会话，仅在当前模式支持安全接管时才自动恢复
- 不满足安全接管条件的会话不会强行恢复持仓监控，避免升级后误接管

## API 与通知配置

应用支持保存多套 API 凭证 profile，并在界面里按 profile 切换使用。

邮件通知仍然使用一套全局 SMTP / 收件人配置，但现在可以额外给每个 API profile 单独指定“发件邮箱”覆盖：

- 全局 `发件邮箱` 继续作为默认值
- `当前 API 专属发件邮箱` 只对当前选中的 API profile 生效
- 留空时自动回退到全局 `发件邮箱`
- 重命名或删除 API profile 时，对应的专属发件邮箱映射也会同步调整

相关配置会保存在 `qqokx_data/config/` 下的用户设置中，便于升级后继续沿用。

## 核心模块

### 策略与交易

- [okx_quant/engine.py](/D:/qqokx/okx_quant/engine.py)：策略执行与交易主引擎
- [okx_quant/engine_strategy_router.py](/D:/qqokx/okx_quant/engine_strategy_router.py)：实盘策略运行路由，按 runtime registry 选择 signal/local/exchange 入口
- [okx_quant/ui_strategy_sessions.py](/D:/qqokx/okx_quant/ui_strategy_sessions.py)：策略会话管理界面
- [okx_quant/ui_positions.py](/D:/qqokx/okx_quant/ui_positions.py)：账户持仓、历史成交、历史仓位与持仓 WS 缓存状态展示
- [okx_quant/smart_order.py](/D:/qqokx/okx_quant/smart_order.py)：Smart Order 任务执行
- [okx_quant/trader_desk.py](/D:/qqokx/okx_quant/trader_desk.py)：交易台能力
- [okx_quant/strategy_ui_schema.py](/D:/qqokx/okx_quant/strategy_ui_schema.py)：策略 UI schema，负责默认值、控件显示隐藏、强制行为
- [okx_quant/strategy_runtime_registry.py](/D:/qqokx/okx_quant/strategy_runtime_registry.py)：策略 runtime registry，负责 family、执行入口、方向偏好、标题 helper
- [okx_quant/strategies/ema55_slope_short.py](/D:/qqokx/okx_quant/strategies/ema55_slope_short.py)：EMA55 斜率做空信号逻辑
- [okx_quant/arbitrage_ui.py](/D:/qqokx/okx_quant/arbitrage_ui.py)：现货套利窗口，包含机会扫描、套利开仓/平仓、交割合约移仓、持仓配对平仓、K 线图表与 API 切换
- [okx_quant/arbitrage/arbitrage_manager.py](/D:/qqokx/okx_quant/arbitrage/arbitrage_manager.py)：套利扫描、开平仓、自动监控总入口
- [okx_quant/arbitrage/arbitrage_executor.py](/D:/qqokx/okx_quant/arbitrage/arbitrage_executor.py)：套利开仓/平仓执行、部分平仓与成交回报校验
- [okx_quant/arbitrage/arbitrage_scanner.py](/D:/qqokx/okx_quant/arbitrage/arbitrage_scanner.py)：现货 vs 永续 / 交割扫描、年化比较与类型标签生成
- [okx_quant/okx_private_ws.py](/D:/qqokx/okx_quant/okx_private_ws.py)：OKX 私有 WebSocket 缓存层，当前用于订单、持仓、账户状态加速
- [okx_quant/okx_public_ws.py](/D:/qqokx/okx_quant/okx_public_ws.py)：OKX 公共 WebSocket 行情缓存层，当前用于本地现货套利窗口双盘口实时刷新

### 回测与研究

- [okx_quant/backtest.py](/D:/qqokx/okx_quant/backtest.py)：回测核心逻辑，当前已统一支持 `nR 保本`、纯本地回测、四段动态保护，以及可配置的 `dynamic_protection_rules`
- [okx_quant/backtest_ui.py](/D:/qqokx/okx_quant/backtest_ui.py)：回测界面，当前已支持动态保护规则编辑、运行编号/归档编号区分、以及新的回测 K 线图交互
- [okx_quant/backtest_audit.py](/D:/qqokx/okx_quant/backtest_audit.py)：回测审计导出，当前已改成流式 CSV 写出
- [okx_quant/candle_store.py](/D:/qqokx/okx_quant/candle_store.py)：本地 K 线存储，当前已支持查询缓存根数与时间覆盖范围，以及过期未确认 K 线自动转确认
- [okx_quant/strategy_symbol_defaults.py](/D:/qqokx/okx_quant/strategy_symbol_defaults.py)：分币种策略默认模板，当前已固化 `v0.6.51` 的多头/空头独立参数
- [okx_quant/multi_coin_market_digest.py](/D:/qqokx/okx_quant/multi_coin_market_digest.py)：多币种市场早报，当前已支持“明确观点 + 最近复盘命中率”邮件内容
- [okx_quant/analysis_email_validation.py](/D:/qqokx/okx_quant/analysis_email_validation.py)：多币种早报邮件的历史观点回放验证与汇总导出
- [reports/ema55_slope_short_research_report.html](/D:/qqokx/reports/ema55_slope_short_research_report.html)：EMA55 斜率做空研究报告（HTML）
- [scripts/run_ema55_slope_short_research_report.py](/D:/qqokx/scripts/run_ema55_slope_short_research_report.py)：EMA55 斜率做空研究复跑脚本
- [okx_quant/btc_market_analyzer.py](/D:/qqokx/okx_quant/btc_market_analyzer.py)：BTC 市场研究分析
- [okx_quant/btc_research_workbench_ui.py](/D:/qqokx/okx_quant/btc_research_workbench_ui.py)：BTC 研究工作台

### 波动率与期权

- [okx_quant/deribit_volatility_monitor.py](/D:/qqokx/okx_quant/deribit_volatility_monitor.py)：波动率监控
- [okx_quant/option_strategy.py](/D:/qqokx/okx_quant/option_strategy.py)：期权策略计算
- [okx_quant/option_roll.py](/D:/qqokx/okx_quant/option_roll.py)：期权展期建议
- [okx_quant/position_protection.py](/D:/qqokx/okx_quant/position_protection.py)：期权保护

## 常用脚本

[scripts](/D:/qqokx/scripts) 目录下保留了常用工具：

- `scripts/release_one_click.ps1`：一键发版
- `scripts/release_one_click.bat`：Windows 命令行发版入口
- `scripts/build_server_package.py`：打包
- `scripts/run_moni_arbitrage_smoke.py`：`moni/demo` 账户现货套利冒烟测试脚本
- `scripts/run_btc_market_analysis.py`：BTC 研究分析入口
- `scripts/generate_comprehensive_backtest_report.py`：综合回测报告生成
- `scripts/check_local_candle_gaps.py`：本地 K 线缺口检查
- `scripts/fill_local_candle_gaps.py`：本地 K 线缺口补齐
- `scripts/run_ema55_slope_short_research_report.py`：EMA55 斜率做空研究报告复跑
- `scripts/run_btc_s089_daily_ema_compare_report.py`：BTC `S089` 日线 EMA 对比报告
- `scripts/research_btc_s096_s097_distance_confirmation_compare.py`：`S096/S097` 距离确认对比研究
- `scripts/research_btc_s096_s097_min_distance_window_report.py`：`S096/S097` 最小距离窗口研究
- `scripts/research_btc_s097_far_entry_early_protection_report.py`：`S097` 远距入场提前保护研究
- `scripts/research_btc_s097_loss_archetype_report.py`：`S097` 亏损形态归因研究
- `scripts/research_btc_s097_protective_reentry_report.py`：`S097` 保护后再入场研究
- `scripts/build_best_parameter_bundle.py`：生成最佳参数组合包与对应 HTML 说明
- `scripts/run_best_long_trigger_r_experiment.py`：最佳多头组合的 `首档触发R` 对比实验
- `scripts/run_best_short_line_experiment.py`：最佳空头组合的均线类型 / 周期对比实验
- `scripts/run_best_parameter_bundle_overall_report.py`：最佳参数组合包整体表现汇总
- `scripts/generate_s140_s160_strategy_analysis.py`：`S140-S160` 策略分析报告
- `scripts/generate_btc_long_5_software_analysis.py`：BTC 多头最近 5 组软件回测结果分析
- `scripts/run_btc_long_three_config_full_compare.py`：BTC 多头 3 套配置全量对比
- `scripts/run_r001_r003_local_full_compare_report.py`：`R001-R003` 本地全量重跑对比报告
- `scripts/run_btc_dynamic_long_ma50_vs_ema55_matrix.py`：BTC 动态委托做多 `MA50 vs EMA55` 参数矩阵对比
- `scripts/run_eth_dynamic_long_template_refine_ema55.py`：ETH 动态委托做多模板打磨
- `scripts/run_eth_sol_doge_dynamic_long_template_refine.py`：ETH / SOL / DOGE 动态委托做多模板联合打磨
- `scripts/run_sol_slope_short_refine.py`：SOL 斜率做空模板打磨
- `scripts/run_doge_slope_short_refine.py`：DOGE 斜率做空模板打磨
- `scripts/run_multi_coin_email_validation.py`：多币种市场早报邮件历史观点验证
- `scripts/generate_doge_dynamic_long_s652_s655_review.py`：DOGE 动态做多 `S652-S655` 复核
- `scripts/run_sol_dynamic_long_lock5_validation.py`：SOL 动态做多 `5R 锁 1R` 验证
- `scripts/run_sol_dynamic_long_s656_followup.py`：SOL 动态做多 `S656` 跟进实验

## 现货套利快速上手

适合第一次使用现货套利窗口时快速走通主流程：

1. 启动主程序后，打开“现货套利”窗口，并先在顶部切换好 `API profile` 与 `实盘 / 模拟盘`。
2. 在“机会扫描”页勾选 `永续` 或 `交割`，按需要选择 `币种`、排序列，然后点击“立即扫描”查看机会列表。
3. 在“套利开仓”页填写：
   - `币种`：例如 `BTC`
   - `衍生品`：例如 `BTC-USD-260925` 或 `BTC-USDT-SWAP`
   - `投入数量`：支持按 `币数 / USDT / 合约张数`
   - `触发方式`：默认 `绝对价差触发`
   - `开仓绝对价差 >`：价差扩大到阈值以上时开仓
   - `平仓绝对价差 <`：价差收敛到阈值以下时平仓
4. 先点“刷新预览”确认现货腿、合约腿和名义价值，再根据需要选择：
   - “立即开仓”：手动执行一次
   - “启动自动开仓”：按设定价差持续监控并触发
   - `分批次数 / 每批张数`：开仓会先换算总合约张数，再按这里拆批执行
   - `双腿执行`：支持 `双腿吃单`、`现货挂单/合约吃单`、`合约挂单/现货吃单`
   - `挂单等待 / 追单次数`：只在挂单腿 + 吃单腿模式下生效
5. 开仓后可在“套利平仓”页基于套利账本做平仓；如果仓位不是本工具开的，去“持仓配对平仓”页，直接从当前账户持仓里选择 `现货腿 + 交割/永续腿` 配对平仓。
   - `套利平仓` 现在也支持 `分批次数 / 每批张数`，会按你这次要平的总张数拆批执行
6. “持仓配对平仓”支持手动平仓，也支持设置 `绝对价差 < 阈值` 自动平仓；还可以配置 `分批次数 / 每批张数 / 执行方式 / 挂单等待 / 追单次数`。
7. “交割移仓”页可以基于已有交割合约套利持仓，把旧交割合约回补掉，再开出更远的交割合约；现货腿不动，支持 `分批次数 / 每批张数 / 双腿吃单 / 一腿挂单一腿吃单`。
8. “套利开仓 / 套利平仓 / 持仓配对平仓 / 交割移仓”都带双盘口，优先走 `公共 WS` 实时刷新，拿不到时会自动回退 `REST`。
9. `自动开仓 / 自动平仓 / 自动配对平仓` 的价差判断现在也会优先使用 `公共 WS` 缓存行情，再回退到 `REST`。
10. 如果想观察两条腿走势，可在“套利图表”页加载 `现货 K 线`、`衍生品 K 线` 和 `绝对价差 K 线`。

更详细的字段说明和演示请直接打开 [reports/arbitrage_user_guide.html](/D:/qqokx/reports/arbitrage_user_guide.html)。

## 测试

运行全部测试：

```powershell
python -m pytest
```

常见模块测试示例：

```powershell
python -m pytest tests/test_strategy_engine.py
python -m pytest tests/test_smart_order.py
python -m pytest tests/test_trader_desk.py
python -m pytest tests/test_backtest.py
```

如果这次改动涉及：

- 现货套利
- OKX 私有 WebSocket
- 持仓 / 会话状态展示

建议额外执行：

```powershell
python -m unittest tests.test_okx_client_orders tests.test_engine_retry_policy tests.test_market_data_hub tests.test_arbitrage tests.test_position_protection -v
python -m py_compile D:\qqokx\okx_quant\arbitrage_ui.py D:\qqokx\okx_quant\okx_client.py D:\qqokx\okx_quant\engine_retry_policy.py D:\qqokx\okx_quant\market_data_hub.py D:\qqokx\okx_quant\ui_positions.py D:\qqokx\okx_quant\ui_strategy_sessions.py
```

测试文件位于 [tests](/D:/qqokx/tests)。

## 环境变量

仓库提供了示例文件 [.env.example](/D:/qqokx/.env.example) 用于对齐配置项。当前程序不会自动加载项目根目录 `.env`，请使用系统环境变量或在启动终端中显式设置。

弱网或 VPN 环境下，可通过以下变量调整 OKX 读请求重试：

| 变量 | 默认示例值 | 说明 |
| --- | --- | --- |
| `QQOKX_READ_RETRY_ATTEMPTS` | `16` | 最大重试次数 |
| `QQOKX_READ_RETRY_BASE_DELAY_SECONDS` | `1.5` | 初始退避秒数 |
| `QQOKX_READ_RETRY_MAX_DELAY_SECONDS` | `24` | 最大退避秒数 |
| `QQOKX_PRIVATE_WS_ENABLED` | `1` | 是否启用 OKX 私有 WebSocket 加速订单/持仓/账户状态；设为 `0` 时完全回退 REST |
| `QQOKX_PUBLIC_WS_ENABLED` | `1` | 是否启用 OKX 公共 WebSocket 加速本地套利窗口盘口；设为 `0` 时回退 REST 轮询 |

当前推荐架构是：

- `1H` 级主策略、K 线驱动、普通扫描：继续使用 `REST`
- `订单状态 / 成交回报 / 持仓变化`：优先使用 `私有 WS`
- `触发价 / 公共 ticker / 本地现货套利窗口盘口`：优先使用 `公共 WS`，失败时自动回退 `REST`
- `同一币种同一周期 K 线`：优先使用进程内共享 `market_data_hub`，底层仍按需回退 `REST`

这样可以尽量减少服务器端复杂度，同时把最有价值的“交易后状态”先提速。

PowerShell 示例：

```powershell
$env:QQOKX_READ_RETRY_ATTEMPTS = "16"
$env:QQOKX_READ_RETRY_BASE_DELAY_SECONDS = "1.5"
$env:QQOKX_READ_RETRY_MAX_DELAY_SECONDS = "24"
python main.py
```

## 发版

常用方式：

```powershell
scripts\release_one_click.bat -DryRun
scripts\release_one_click.bat
```

常见参数：

- `-DryRun`：只预演
- `-Bump patch`：补丁版本
- `-Bump minor`：新增明显功能
- `-Bump major`：大版本切换
- `-SkipBuild`：跳过打包
- `-SkipPush`：只提交不推送

详细说明见 [发版协作约定.md](/D:/qqokx/发版协作约定.md) 和 [发版待打包清单.md](/D:/qqokx/发版待打包清单.md)。

## 相关文档

需要查看更细的业务说明、研究记录或协作文档时，可以从以下文件继续进入：

- [reports/arbitrage_user_guide.html](/D:/qqokx/reports/arbitrage_user_guide.html)
  ：现货套利使用说明，包含各填写框解释与开仓/平仓/持仓配对平仓演示
- [reports/arbitrage_moni_test_report.html](/D:/qqokx/reports/arbitrage_moni_test_report.html)
  ：`moni/demo` 账户真实测试报告，记录机会扫描、套利开仓、套利平仓与持仓配对平仓验证结果
- [reports/server_upgrade_checklist.html](/D:/qqokx/reports/server_upgrade_checklist.html)
  ：服务器升级操作清单，适合按实盘环境灰度启用私有 WS 加速
- [软件开发指南.md](/D:/qqokx/软件开发指南.md)
  ：开发维护说明，已补充策略 schema / runtime registry、EMA55 斜率做空、回测与 UI 接入约定
- [版本开发日志_v0.6.51.md](/D:/qqokx/版本开发日志_v0.6.51.md)
  ：本轮版本开发日志，归档 EMA55 策略、研究报告、B 方案结构重构与验证结果
- [reports/strategy_ui_schema_b_impl.md](/D:/qqokx/reports/strategy_ui_schema_b_impl.md)
  ：B 方案实施说明，记录 schema / registry 这一轮已经解掉的耦合和剩余尾项
- [reports/strategy_isolation_plan.md](/D:/qqokx/reports/strategy_isolation_plan.md)
  ：“新策略与主程序解耦”设计方案与推进顺序
- [线程工作流模板.md](/D:/qqokx/线程工作流模板.md)
- [docs/archive/自动通道系统_v1_产品需求与技术路线.md](/D:/qqokx/docs/archive/自动通道系统_v1_产品需求与技术路线.md)
- [docs/archive/BTC研究工作台开发记录.md](/D:/qqokx/docs/archive/BTC研究工作台开发记录.md)
- [docs/archive/交易员晨会解读.md](/D:/qqokx/docs/archive/交易员晨会解读.md)
