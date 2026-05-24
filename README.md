# OKX 策略工作台

当前版本：`v0.6.03`

一个面向 OKX 的桌面量化交易工作台，围绕策略运行、交易辅助、回测研究和分析导出构建，适合做策略验证、实盘辅助和研究沉淀。

## 项目概览

当前仓库主要包含以下能力：

- 多策略会话运行与恢复接管
- 持仓、历史成交、历史仓位查看
- Smart Order、条件单、网格类交易辅助
- 期权仓位保护、期权策略计算、展期建议
- Deribit 波动率查看与监控
- 回测、参数矩阵对比、结果持久化
- BTC 研究工作台与报告导出

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
- `state/`：策略状态、回测历史、Smart Order 任务
- `logs/`：运行日志与策略日志
- `reports/`：研究导出、分析报告、实盘导出

兼容迁移规则：

- 首次启动会尝试迁移旧版 `.okx_quant_*` 文件以及原有 `logs/`、`reports/`
- 升级时建议只替换代码目录，保留原 `qqokx_data/`

## 核心模块

### 策略与交易

- [okx_quant/engine.py](/D:/qqokx/okx_quant/engine.py)：策略执行与交易主引擎
- [okx_quant/ui_strategy_sessions.py](/D:/qqokx/okx_quant/ui_strategy_sessions.py)：策略会话管理界面
- [okx_quant/smart_order.py](/D:/qqokx/okx_quant/smart_order.py)：Smart Order 任务执行
- [okx_quant/trader_desk.py](/D:/qqokx/okx_quant/trader_desk.py)：交易台能力

### 回测与研究

- [okx_quant/backtest.py](/D:/qqokx/okx_quant/backtest.py)：回测核心逻辑
- [okx_quant/backtest_ui.py](/D:/qqokx/okx_quant/backtest_ui.py)：回测界面
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
- `scripts/run_btc_market_analysis.py`：BTC 研究分析入口
- `scripts/generate_comprehensive_backtest_report.py`：综合回测报告生成
- `scripts/check_local_candle_gaps.py`：本地 K 线缺口检查
- `scripts/fill_local_candle_gaps.py`：本地 K 线缺口补齐

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

测试文件位于 [tests](/D:/qqokx/tests)。

## 环境变量

仓库提供了示例文件 [.env.example](/D:/qqokx/.env.example) 用于对齐配置项。当前程序不会自动加载项目根目录 `.env`，请使用系统环境变量或在启动终端中显式设置。

弱网或 VPN 环境下，可通过以下变量调整 OKX 读请求重试：

| 变量 | 默认示例值 | 说明 |
| --- | --- | --- |
| `QQOKX_READ_RETRY_ATTEMPTS` | `16` | 最大重试次数 |
| `QQOKX_READ_RETRY_BASE_DELAY_SECONDS` | `1.5` | 初始退避秒数 |
| `QQOKX_READ_RETRY_MAX_DELAY_SECONDS` | `24` | 最大退避秒数 |

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

- [软件开发指南.md](/D:/qqokx/软件开发指南.md)
- [线程工作流模板.md](/D:/qqokx/线程工作流模板.md)
- [自动通道系统_v1_产品需求与技术路线.md](/D:/qqokx/自动通道系统_v1_产品需求与技术路线.md)
- [BTC研究工作台开发记录.md](/D:/qqokx/BTC研究工作台开发记录.md)
- [交易员晨会解读.md](/D:/qqokx/交易员晨会解读.md)
