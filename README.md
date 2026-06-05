# OKX 策略工作台

当前版本：`v0.6.12`

一个面向 OKX 的桌面量化交易工作台，围绕策略运行、交易辅助、回测研究和分析导出构建，适合做策略验证、实盘辅助和研究沉淀。

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

`v0.6.12` 之后这一轮版本内容比较集中，重点新增和调整如下：

- 新增 `EMA55 斜率做空` 策略：
  - 规则：`EMA55` 单根斜率比例小于等于阈值时开空，斜率重新转正时平仓
  - 支持 launcher / backtest 双端参数
  - 斜率阈值可调，默认值为 `-0.0005`
- `EMA55 斜率做空` 的回测与研究能力补齐：
  - 支持固定风险金、固定数量、风险百分比三类仓位口径
  - 支持 `2R 保本`、手续费偏移、时间保本、动态止盈
  - 输出了独立 HTML 研究报告与多组研究脚本
- 策略接入结构做了 B 方案重构：
  - 新增 `strategy_ui_schema.py`，集中声明策略 UI 默认值、显示隐藏、强制只读/强制行为
  - 新增 `strategy_runtime_registry.py`，集中声明策略 family、运行入口、方向偏好、参考线标题等
  - launcher / backtest / engine / router 开始共用 schema / registry，不再靠大量 `if strategy_id == ...`
- 修复了新策略接入引发的启动崩溃问题，并补充了启动烟测、策略切换回归和运行路由测试
- 回测说明、参数矩阵、标题文案、方向偏好等逻辑开始按 runtime family 统一分流，减少主程序与具体策略的强耦合

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

- [okx_quant/backtest.py](/D:/qqokx/okx_quant/backtest.py)：回测核心逻辑
- [okx_quant/backtest_ui.py](/D:/qqokx/okx_quant/backtest_ui.py)：回测界面
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
python -m unittest tests.test_okx_client_orders tests.test_arbitrage tests.test_position_protection -v
python -m py_compile D:\qqokx\okx_quant\arbitrage_ui.py D:\qqokx\okx_quant\okx_client.py D:\qqokx\okx_quant\ui_positions.py D:\qqokx\okx_quant\ui_strategy_sessions.py
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
- `本地现货套利窗口盘口`：优先使用 `公共 WS`，失败时自动回退 `REST`

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
- [版本开发日志_v0.6.12.md](/D:/qqokx/版本开发日志_v0.6.12.md)
  ：本轮版本开发日志，归档 EMA55 策略、研究报告、B 方案结构重构与验证结果
- [reports/strategy_ui_schema_b_impl.md](/D:/qqokx/reports/strategy_ui_schema_b_impl.md)
  ：B 方案实施说明，记录 schema / registry 这一轮已经解掉的耦合和剩余尾项
- [reports/strategy_isolation_plan.md](/D:/qqokx/reports/strategy_isolation_plan.md)
  ：“新策略与主程序解耦”设计方案与推进顺序
- [线程工作流模板.md](/D:/qqokx/线程工作流模板.md)
- [自动通道系统_v1_产品需求与技术路线.md](/D:/qqokx/自动通道系统_v1_产品需求与技术路线.md)
- [BTC研究工作台开发记录.md](/D:/qqokx/BTC研究工作台开发记录.md)
- [交易员晨会解读.md](/D:/qqokx/交易员晨会解读.md)
