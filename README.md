# OKX 策略工作台

当前版本：`v0.5.43`

一个面向 OKX 的桌面量化交易工作台，围绕“监控、交易、保护、回测、分析”构建，适合做策略验证、实盘辅助和研究产出沉淀。

## 项目定位

项目当前覆盖的核心方向：

- 策略运行与多会话管理
- 账户持仓、历史成交、历史仓位查看
- 期权持仓保护与回放模拟
- 多币种信号监控与通知
- Deribit 波动率指数查看与波动率监控
- 回测、矩阵对比、历史结果持久化
- Smart Order / 条件单 / 网格任务
- 期权策略计算器与展期建议
- BTC 研究工作台与分析导出

仓库主代码位于 [okx_quant](/D:/qqokx/okx_quant)，研究与统计相关代码位于 [research](/D:/qqokx/research)、[stats](/D:/qqokx/stats)、[export](/D:/qqokx/export)。

## 目录结构

```text
qqokx/
├─ main.py                  # 桌面应用入口
├─ okx_quant/               # 主应用与交易/回测/UI 代码
├─ research/                # 研究流水线
├─ stats/                   # 统计与指标计算
├─ export/                  # 报告与 CSV 导出
├─ scripts/                 # 研究、回测、打包、诊断脚本
├─ tests/                   # 自动化测试
└─ dist/                    # 打包输出
```

## 运行要求

- Python `3.11+`
- Windows 桌面环境优先
- 如果在 Linux 远程环境运行界面，需要额外安装 Tk

示例：

```bash
sudo apt-get install -y python3-tk
```

当前项目运行时不依赖额外第三方交易框架；基础依赖以 [pyproject.toml](/D:/qqokx/pyproject.toml) 为准。

## 启动方式

默认启动：

```bash
python main.py
```

指定数据目录启动：

```bash
python main.py --data-dir D:\qqokx_data
```

也可以通过环境变量指定：

```powershell
$env:QQOKX_DATA_DIR = "D:\qqokx_data"
python main.py
```

应用入口在 [main.py](/D:/qqokx/main.py)，数据目录管理逻辑在 [okx_quant/app_paths.py](/D:/qqokx/okx_quant/app_paths.py)。

## 数据目录

运行期数据已经从代码目录中抽离，默认使用代码目录同级的 `qqokx_data/`。

数据目录结构：

- `config/`：API 凭证、设置、通知配置
- `cache/`：本地 K 线缓存、Deribit 波动率缓存
- `state/`：回测历史、策略历史、Smart Order 任务等状态文件
- `logs/`：全局日志与策略日志
- `reports/`：研究导出、实盘会话导出、分析报告

兼容迁移规则：

- 首次启动会自动尝试迁移旧版 `.okx_quant_*` 文件、`logs/` 和 `reports/`
- 升级代码时，推荐只替换代码目录，保留原 `qqokx_data/`

## 核心能力

### 1. 策略工作台

- 支持多策略会话并行运行
- 支持 API 账户切换
- 支持运行日志与会话状态查看
- 支持异常停止后的策略恢复接管

当前仓库已接入的代表性策略包括：

- `EMA 动态委托-多头`
- `EMA 动态委托-空头`
- `EMA 穿越市价`
- `4H EMA5 / EMA8 金叉死叉`
- `现货增强三十六计`

### 2. 持仓与交易辅助

- 当前持仓树形分组与摘要统计
- 历史成交、历史仓位查询
- 期权仓位保护
- Smart Order、条件单、点击报价网格
- 动态止盈接管与多任务并行监控

### 3. 回测与研究

- 多周期 K 线回测
- 参数矩阵对比与热力图
- 手续费、滑点、资金费率建模
- 历史结果持久化
- 研究报告与 CSV/HTML 导出

### 4. 波动率与期权分析

- Deribit 波动率指数查看
- 波动率信号监控
- 期权策略计算器
- 期权展期建议

## 常用脚本

仓库中的 [scripts](/D:/qqokx/scripts) 目录包含常用工具脚本，例如：

- `scripts/release_one_click.ps1`：一键发版
- `scripts/build_server_package.py`：打包
- `scripts/run_btc_market_analysis.py`：BTC 研究工作台分析入口
- `scripts/generate_comprehensive_backtest_report.py`：综合回测报告
- `scripts/check_local_candle_gaps.py`：本地 K 线缺口检查

## 测试

运行全部测试：

```bash
python -m pytest
```

按模块执行示例：

```bash
python -m pytest tests/test_strategy_engine.py
python -m pytest tests/test_smart_order.py
python -m pytest tests/test_trader_desk.py
```

测试目录见 [tests](/D:/qqokx/tests)。

## 环境变量

仓库提供了示例文件 [.env.example](/D:/qqokx/.env.example) 方便对齐配置项。当前程序不会自动加载项目根目录 `.env`，请使用系统环境变量或在启动终端中显式设置。

弱网或 VPN 环境下，OKX 读请求可通过以下变量调整重试：

| 变量 | 默认值 | 建议弱网值 |
| --- | --- | --- |
| `QQOKX_READ_RETRY_ATTEMPTS` | `8` | `16` |
| `QQOKX_READ_RETRY_BASE_DELAY_SECONDS` | `1.0` | `1.5` |
| `QQOKX_READ_RETRY_MAX_DELAY_SECONDS` | `8.0` | `24` |

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

可选参数：

- `-DryRun`：只预演
- `-Bump patch`：小改动
- `-Bump minor`：新增明显功能
- `-Bump major`：版本线切换或大改
- `-SkipBuild`：跳过打包
- `-SkipPush`：只提交不推送

发版协作说明见 [发版协作约定.md](/D:/qqokx/发版协作约定.md)，待打包事项见 [发版待打包清单.md](/D:/qqokx/发版待打包清单.md)。

## 相关文档

如果要看更细的业务说明、研究记录或协作文档，可以直接从这些文件进入：

- [软件开发指南.md](/D:/qqokx/软件开发指南.md)
- [线程工作流模板.md](/D:/qqokx/线程工作流模板.md)
- [自动通道系统_v1_产品需求与技术路线.md](/D:/qqokx/自动通道系统_v1_产品需求与技术路线.md)
- [BTC研究工作台开发记录.md](/D:/qqokx/BTC研究工作台开发记录.md)
- [交易员晨会解读.md](/D:/qqokx/交易员晨会解读.md)

## 维护建议

当前 README 建议只保留：

- 项目定位
- 启动与部署方式
- 数据目录约定
- 核心入口与常用脚本
- 指向详细文档的导航

逐版本长更新日志更适合沉淀到独立 `CHANGELOG` 或发版文档中，避免 README 再次变成“信息仓库”。
