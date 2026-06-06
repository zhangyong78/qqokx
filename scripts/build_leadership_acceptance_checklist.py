from __future__ import annotations

import html
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.persistence import analysis_report_dir_path
from scripts.build_five_coin_daily_filter_operation_pack import ReadyStrategySpec, build_ready_specs


REPORT_DIR = analysis_report_dir_path()
PACKAGE_DIR = REPORT_DIR / "packages"
PACKAGE_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def latest_bundle_path(pattern: str) -> Path | None:
    matches = sorted(PACKAGE_DIR.glob(pattern), key=lambda item: item.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def direction_label(raw: str) -> str:
    return {"long_only": "只做多", "short_only": "只做空", "both": "多空都做"}.get(raw, raw)


def family_label(raw: str) -> str:
    return {
        "dynamic_long": "动态委托做多",
        "slope_short": "斜率做空",
        "body_retest_short": "Body/ATR 回抽做空",
    }.get(raw, raw)


def render_strategy_rows(ready_specs: tuple[ReadyStrategySpec, ...]) -> str:
    rows: list[str] = []
    for index, spec in enumerate(ready_specs, start=1):
        rows.append(
            "<tr>"
            f"<td>{index}</td>"
            f"<td>{html.escape(spec.symbol)}</td>"
            f"<td>{html.escape(direction_label(spec.direction_label))}</td>"
            f"<td>{html.escape(family_label(spec.family))}</td>"
            f"<td>{html.escape(spec.profile_name)}</td>"
            f"<td>{html.escape(spec.hour_summary)}</td>"
            f"<td>{html.escape(spec.daily_summary)}</td>"
            f"<td>{html.escape(spec.notes)}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def build_html(
    *,
    ready_specs: tuple[ReadyStrategySpec, ...],
    demo_bundle: Path | None,
    live_bundle: Path | None,
) -> str:
    generated_at = utc_now_text()
    strategy_rows = render_strategy_rows(ready_specs)
    demo_text = str(demo_bundle) if demo_bundle is not None else "未找到现成 Demo Bundle"
    live_text = str(live_bundle) if live_bundle is not None else "未找到现成 Live Bundle"
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>领导验收清单：五币日线过滤策略</title>
  <style>
    body {{
      margin: 0;
      font-family: "Segoe UI", "Microsoft YaHei UI", sans-serif;
      background: linear-gradient(180deg, #f3efe5 0%, #f8fbff 100%);
      color: #1f2937;
    }}
    .wrap {{
      max-width: 1320px;
      margin: 0 auto;
      padding: 24px 20px 40px;
    }}
    .card {{
      background: #fffdfa;
      border: 1px solid #ddd5c7;
      border-radius: 18px;
      box-shadow: 0 12px 28px rgba(15, 23, 42, 0.06);
      padding: 20px 22px;
      margin-bottom: 16px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-top: 14px;
    }}
    .kpi {{
      border: 1px solid #d7e5e3;
      background: #eef7f6;
      border-radius: 14px;
      padding: 14px 16px;
    }}
    .kpi .label {{
      color: #5b6472;
      font-size: 13px;
      margin-bottom: 8px;
    }}
    .kpi .value {{
      font-size: 22px;
      font-weight: 700;
    }}
    h1, h2 {{
      margin: 0 0 10px;
    }}
    p, li {{
      line-height: 1.72;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
      margin-top: 10px;
    }}
    th, td {{
      border-bottom: 1px solid #e5dfd5;
      text-align: left;
      vertical-align: top;
      padding: 9px 8px;
    }}
    th {{
      background: #f8fafc;
    }}
    .cols {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
    }}
    .checklist td:first-child {{
      width: 190px;
      font-weight: 600;
      color: #374151;
    }}
    code {{
      background: #f3f4f6;
      border-radius: 8px;
      padding: 1px 6px;
      font-family: Consolas, monospace;
    }}
    .tip {{
      background: #fff7ed;
      border-left: 4px solid #b45309;
      border-radius: 12px;
      padding: 12px 14px;
      margin-top: 12px;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="card">
      <h1>领导验收清单：五币日线过滤策略</h1>
      <p>生成时间：{html.escape(generated_at)}</p>
      <p>本页用于领导验收时快速确认：<code>10 条正式策略</code>、<code>Bundle 导入流程</code>、<code>回测填写口径</code>、<code>实盘启动口径</code> 是否全部统一。</p>
      <div class="grid">
        <div class="kpi"><div class="label">正式策略数量</div><div class="value">10 条</div></div>
        <div class="kpi"><div class="label">默认风险口径</div><div class="value">每笔 10U</div></div>
        <div class="kpi"><div class="label">默认回测本金</div><div class="value">10000U</div></div>
        <div class="kpi"><div class="label">默认日线边界</div><div class="value">北京时间 8 点</div></div>
      </div>
      <div class="tip">
        核心审计标准：所有日线过滤都只能读取“当时上一根已经收盘的日线”，不允许读取未收盘当日线。
      </div>
    </section>

    <section class="card">
      <h2>交付文件</h2>
      <table class="checklist">
        <tbody>
          <tr><td>Demo Bundle</td><td>{html.escape(demo_text)}</td></tr>
          <tr><td>Live Bundle</td><td>{html.escape(live_text)}</td></tr>
          <tr><td>推荐导入顺序</td><td>先导入 Demo Bundle，核对参数、日线过滤摘要和策略清单无误后，再导入 Live Bundle。</td></tr>
        </tbody>
      </table>
    </section>

    <section class="card">
      <h2>10 条正式策略清单</h2>
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>币种</th>
            <th>方向</th>
            <th>策略家族</th>
            <th>策略名称</th>
            <th>1H 核心参数</th>
            <th>日线过滤</th>
            <th>补充说明</th>
          </tr>
        </thead>
        <tbody>
          {strategy_rows}
        </tbody>
      </table>
    </section>

    <section class="card">
      <h2>回测与实盘统一口径</h2>
      <div class="cols">
        <table class="checklist">
          <thead><tr><th colspan="2">回测填写口径</th></tr></thead>
          <tbody>
            <tr><td>K 线周期</td><td>统一填 <code>1H</code>。</td></tr>
            <tr><td>仓位/风险</td><td>统一按 <code>fixed_risk</code>，每笔 <code>10U</code>，初始资金 <code>10000U</code>。</td></tr>
            <tr><td>日线边界</td><td>统一选 <code>BJT 08:00</code>，除非专项对比研究明确改为别的边界。</td></tr>
            <tr><td>日线规则</td><td>多头用 <code>close_vs_ma</code>，空头按各策略表填写；BNB 专用空头用 <code>weak_day</code>。</td></tr>
            <tr><td>对齐标准</td><td>只能使用当时上一根已收盘日线，不允许看当天未收盘日线。</td></tr>
            <tr><td>核对重点</td><td>确认策略 ID、方向、多空参数、日线过滤摘要与本页完全一致。</td></tr>
          </tbody>
        </table>

        <table class="checklist">
          <thead><tr><th colspan="2">实盘启动口径</th></tr></thead>
          <tbody>
            <tr><td>导入方式</td><td>支持全部导入，也支持按币种、按方向或逐条勾选部分导入。</td></tr>
            <tr><td>API 映射</td><td>支持保留原 API、统一改当前 API、统一改指定 API、逐条指定 API。</td></tr>
            <tr><td>启动前检查</td><td>先核对 API、方向、风险金、日线边界和过滤规则，再决定是否自动启动。</td></tr>
            <tr><td>上线顺序</td><td>建议先 <code>dry-run</code> 或模拟盘，再切到 live。</td></tr>
            <tr><td>日志确认</td><td>启动日志里必须能看到日线过滤摘要，且与本页和 Bundle 预览一致。</td></tr>
            <tr><td>图表确认</td><td>实时图应能看到日线边界标记与过滤摘要，便于交易员复核。</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h2>领导验收清单</h2>
      <table class="checklist">
        <tbody>
          <tr><td>策略数量</td><td>确认 Bundle 内为 10 条，不多不少，且包含 BNB 的 <code>body_retest_short</code>。</td></tr>
          <tr><td>参数一致性</td><td>确认回测填写、Bundle 预览、实盘启动页三处看到的参数完全一致。</td></tr>
          <tr><td>日线过滤</td><td>确认默认边界为北京时间 8 点，且使用上一根已收盘日线。</td></tr>
          <tr><td>部分导入</td><td>确认可以只导入部分策略，且 API 映射不会串错。</td></tr>
          <tr><td>回测复核</td><td>随机抽 1 到 2 条策略，核对回测参数与本页表格一致。</td></tr>
          <tr><td>实盘复核</td><td>随机抽 1 到 2 条策略，核对启动日志和实时图中的过滤摘要一致。</td></tr>
          <tr><td>审计结论</td><td>确认不存在“未收盘日线 close 提前可见”的未来数据问题。</td></tr>
        </tbody>
      </table>
    </section>
  </div>
</body>
</html>
"""


def main() -> None:
    ready_specs = build_ready_specs("live")
    demo_bundle = latest_bundle_path("five_coin_daily_filter_ready10_bjt08_demo_*.json")
    live_bundle = latest_bundle_path("five_coin_daily_filter_ready10_bjt08_live_*.json")
    output_path = PACKAGE_DIR / f"leadership_acceptance_checklist_{STAMP}.html"
    output_path.write_text(
        build_html(
            ready_specs=ready_specs,
            demo_bundle=demo_bundle,
            live_bundle=live_bundle,
        ),
        encoding="utf-8",
    )
    print(output_path)


if __name__ == "__main__":
    main()
