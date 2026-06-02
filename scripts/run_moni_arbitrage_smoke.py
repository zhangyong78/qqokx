from __future__ import annotations

import html
import time
import traceback
import threading
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from okx_quant.arbitrage.arbitrage_executor import (
    ArbitrageCloseRequest,
    ArbitrageOpenRequest,
    _build_strategy_config,
    _wait_order_fill,
)
from okx_quant.arbitrage.arbitrage_manager import ArbitrageManager
from okx_quant.arbitrage.basis_calculator import mid_price
from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.arbitrage.position_ledger import find_ledger_entry, load_open_ledger_entries
from okx_quant.arbitrage_ui import (
    ArbitrageWindow,
    _build_spot_positions_from_account,
    _pair_position_direction,
    _pair_spot_qty_from_derivative_qty,
)
from okx_quant.models import Credentials
from okx_quant.okx_client import OkxRestClient, OkxPosition
from okx_quant.persistence import load_credentials_snapshot
from okx_quant.pricing import format_decimal, snap_to_increment


PROFILE_NAME = "moni"
ENVIRONMENT = "demo"
BASE_CCY = "BTC"
SPOT_INST_ID = "BTC-USDT"
OPEN_DERIVATIVE_INST_ID = "BTC-USDT-260626"
PAIR_CLOSE_DERIVATIVE_INST_ID = "BTC-USD-260626"
MAX_SLIPPAGE = Decimal("0.0015")
REPORT_PATH = Path("D:/qqokx/reports/arbitrage_moni_test_report.html")

EXECUTION_MODES: list[tuple[str, str]] = [
    ("双腿吃单", "dual_taker"),
    ("现货挂单/合约吃单", "spot_maker_derivative_taker"),
    ("合约挂单/现货吃单", "derivative_maker_spot_taker"),
]


class _Var:
    def __init__(self, value):
        self._value = value

    def get(self):
        return self._value

    def set(self, value):
        self._value = value


class _DummyWindow:
    def after(self, _delay_ms: int, func):
        func()

    @staticmethod
    def winfo_exists() -> bool:
        return True


@dataclass
class StepResult:
    title: str
    success: bool
    details: str


class HtmlReport:
    def __init__(self) -> None:
        self.sections: list[tuple[str, list[StepResult]]] = []

    def add_section(self, title: str, rows: list[StepResult]) -> None:
        self.sections.append((title, rows))

    def save(self, path: Path, *, started_at: str, finished_at: str) -> None:
        blocks: list[str] = []
        for section_title, rows in self.sections:
            cards: list[str] = []
            for row in rows:
                status_text = "通过" if row.success else "失败"
                status_class = "ok" if row.success else "bad"
                cards.append(
                    "<article class='card'>"
                    f"<div class='head'><h3>{html.escape(row.title)}</h3>"
                    f"<span class='status {status_class}'>{status_text}</span></div>"
                    f"<pre>{html.escape(row.details)}</pre>"
                    "</article>"
                )
            blocks.append(
                "<section class='section'>"
                f"<h2>{html.escape(section_title)}</h2>"
                f"<div class='grid'>{''.join(cards)}</div>"
                "</section>"
            )

        content = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>moni 账户套利测试报告</title>
  <style>
    :root {{
      --bg: #f5f7fb;
      --paper: rgba(255, 255, 255, 0.96);
      --ink: #1f2937;
      --muted: #6b7280;
      --line: #dde5ef;
      --ok: #166534;
      --ok-bg: #dcfce7;
      --bad: #991b1b;
      --bad-bg: #fee2e2;
      --info: #075985;
      --info-bg: #e0f2fe;
      --shadow: 0 18px 40px rgba(31, 41, 55, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Microsoft YaHei UI", "PingFang SC", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, rgba(59, 130, 246, 0.08), transparent 26%),
        radial-gradient(circle at left 20%, rgba(16, 185, 129, 0.08), transparent 22%),
        var(--bg);
      line-height: 1.65;
    }}
    .page {{
      max-width: 1220px;
      margin: 0 auto;
      padding: 28px 18px 60px;
    }}
    .hero, .section {{
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: var(--shadow);
      padding: 24px 26px;
      margin-bottom: 18px;
    }}
    .hero h1 {{
      margin: 0;
      font-size: 36px;
      line-height: 1.2;
    }}
    .hero p {{
      margin: 10px 0 0;
      color: var(--muted);
    }}
    .chips {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 16px;
    }}
    .chip {{
      padding: 8px 12px;
      border-radius: 999px;
      font-size: 13px;
      font-weight: 700;
      background: var(--info-bg);
      color: var(--info);
    }}
    .section h2 {{
      margin: 0 0 12px;
      font-size: 24px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
      margin-top: 16px;
    }}
    .card {{
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
      background: #fff;
    }}
    .head {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 10px;
    }}
    .card h3 {{
      margin: 0;
      font-size: 18px;
    }}
    .status {{
      display: inline-block;
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
    }}
    .ok {{ color: var(--ok); background: var(--ok-bg); }}
    .bad {{ color: var(--bad); background: var(--bad-bg); }}
    pre {{
      margin: 0;
      padding: 12px 14px;
      border-radius: 14px;
      border: 1px solid #e9eef5;
      background: #f8fafc;
      white-space: pre-wrap;
      word-break: break-word;
      font-size: 13px;
      line-height: 1.6;
    }}
    @media (max-width: 920px) {{
      .grid {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>moni 账户套利功能测试报告</h1>
      <p>账户：<strong>{PROFILE_NAME}</strong> | 环境：<strong>{ENVIRONMENT}</strong></p>
      <p>覆盖范围：机会扫描、套利开仓三种数量单位、三种执行方式、套利平仓、自动开平仓、持仓配对平仓手动/自动链路。</p>
      <div class="chips">
        <span class="chip">开始：{html.escape(started_at)}</span>
        <span class="chip">结束：{html.escape(finished_at)}</span>
        <span class="chip">开平仓现货：{SPOT_INST_ID}</span>
        <span class="chip">套利开平仓合约：{OPEN_DERIVATIVE_INST_ID}</span>
        <span class="chip">配对平仓合约：{PAIR_CLOSE_DERIVATIVE_INST_ID}</span>
      </div>
    </section>
    {''.join(blocks)}
  </div>
</body>
</html>"""
        path.write_text(content, encoding="utf-8")


def wait_for(predicate, timeout_seconds: float = 120.0, interval_seconds: float = 1.0) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_seconds)
    return False


def load_runtime() -> tuple[OkxRestClient, ArbitrageManager, ArbitrageTradeRuntime]:
    snapshot = load_credentials_snapshot(profile_name=PROFILE_NAME)
    credentials = Credentials(
        snapshot["api_key"],
        snapshot["secret_key"],
        snapshot["passphrase"],
        profile_name=PROFILE_NAME,
    )
    runtime = ArbitrageTradeRuntime(
        credentials=credentials,
        environment=ENVIRONMENT,
        trade_mode="cross",
        position_mode="net",
        credential_profile_name=PROFILE_NAME,
    )
    client = OkxRestClient()
    manager = ArbitrageManager(client, logger=lambda _message: None)
    return client, manager, runtime


def position_snapshot(client: OkxRestClient, runtime: ArbitrageTradeRuntime) -> tuple[list[OkxPosition], list[str]]:
    positions = list(client.get_positions(runtime.credentials, environment=runtime.environment))
    labels = [f"{item.inst_id} | {item.inst_type} | pos={item.position} | avail={item.avail_position}" for item in positions]
    return positions, labels


def account_spot_positions(client: OkxRestClient, runtime: ArbitrageTradeRuntime) -> list[OkxPosition]:
    overview = client.get_account_overview(runtime.credentials, environment=runtime.environment)
    return _build_spot_positions_from_account(overview, client)


def current_spread_abs(client: OkxRestClient, derivative_inst_id: str) -> Decimal:
    spot = client.get_ticker(SPOT_INST_ID)
    derivative = client.get_ticker(derivative_inst_id)
    spot_mid = mid_price(spot.bid, spot.ask)
    derivative_mid = mid_price(derivative.bid, derivative.ask)
    if spot_mid is None or derivative_mid is None:
        raise RuntimeError("无法计算当前绝对价差。")
    return derivative_mid - spot_mid


def make_open_request(
    size: Decimal,
    unit: str,
    *,
    derivative_inst_id: str = OPEN_DERIVATIVE_INST_ID,
    execution_mode: str = "dual_taker",
    maker_wait_seconds: float = 4.0,
    chase_limit: int = 2,
) -> ArbitrageOpenRequest:
    return ArbitrageOpenRequest(
        base_ccy=BASE_CCY,
        spot_inst_id=SPOT_INST_ID,
        derivative_inst_id=derivative_inst_id,
        size=size,
        size_unit=unit,
        trigger_mode="spread_abs",
        open_spread_pct_max=None,
        open_spread_abs_max=Decimal("1"),
        spot_limit_price=None,
        derivative_limit_price=None,
        use_limit_orders=False,
        max_slippage=MAX_SLIPPAGE,
        execution_mode=execution_mode,
        maker_wait_seconds=maker_wait_seconds,
        chase_limit=chase_limit,
    )


def make_close_request(
    entry_id: str,
    *,
    execution_mode: str = "dual_taker",
    maker_wait_seconds: float = 4.0,
    chase_limit: int = 2,
    close_derivative_qty: Decimal | None = None,
) -> ArbitrageCloseRequest:
    return ArbitrageCloseRequest(
        entry_id=entry_id,
        max_slippage=MAX_SLIPPAGE,
        use_limit_orders=False,
        execution_mode=execution_mode,
        maker_wait_seconds=maker_wait_seconds,
        chase_limit=chase_limit,
        close_derivative_qty=close_derivative_qty,
    )


def summarize_entry(entry_id: str | None) -> str:
    if not entry_id:
        return "没有 ledger_entry_id"
    entry = find_ledger_entry(entry_id)
    if entry is None:
        return f"账本中未找到 {entry_id}"
    return (
        f"entry_id={entry.entry_id}\n"
        f"spot_qty={format_decimal(entry.spot_qty)}\n"
        f"derivative_qty={format_decimal(entry.derivative_qty)}\n"
        f"spot_inst={entry.spot_inst_id}\n"
        f"derivative_inst={entry.derivative_inst_id}\n"
        f"close_mode={entry.close_mode}"
    )


def open_and_close_manual(
    manager: ArbitrageManager,
    runtime: ArbitrageTradeRuntime,
    *,
    title: str,
    size: Decimal,
    unit: str,
    derivative_inst_id: str = OPEN_DERIVATIVE_INST_ID,
    open_execution_mode: str = "dual_taker",
    close_execution_mode: str = "dual_taker",
) -> StepResult:
    try:
        request = make_open_request(size, unit, derivative_inst_id=derivative_inst_id, execution_mode=open_execution_mode)
        preview = manager.preview_size(
            base_ccy=BASE_CCY,
            derivative_inst_id=derivative_inst_id,
            size=size,
            unit=unit,
        )
        opened = manager.open_now(request, runtime=runtime)
        if not opened.success or not opened.ledger_entry_id:
            return StepResult(
                title,
                False,
                "\n".join(
                    [
                        f"size={size} | unit={unit}",
                        f"derivative_inst={derivative_inst_id}",
                        f"open_execution_mode={open_execution_mode}",
                        f"preview_spot={format_decimal(preview.spot_base_qty)}",
                        f"preview_contracts={format_decimal(preview.swap_contracts)}",
                        f"message={opened.message}",
                    ]
                ),
            )
        close_result = manager.close_now(
            make_close_request(opened.ledger_entry_id, execution_mode=close_execution_mode),
            runtime=runtime,
        )
        return StepResult(
            title,
            close_result.success,
            "\n".join(
                [
                    f"size={size} | unit={unit}",
                    f"derivative_inst={derivative_inst_id}",
                    f"open_execution_mode={open_execution_mode}",
                    f"close_execution_mode={close_execution_mode}",
                    f"preview_spot={format_decimal(preview.spot_base_qty)}",
                    f"preview_contracts={format_decimal(preview.swap_contracts)}",
                    f"open_success={opened.success}",
                    f"open_message={opened.message}",
                    summarize_entry(opened.ledger_entry_id),
                    f"close_success={close_result.success}",
                    f"close_message={close_result.message}",
                ]
            ),
        )
    except Exception as exc:
        return StepResult(title, False, f"{exc}\n\n{traceback.format_exc()}")


def test_manual_partial_close(
    manager: ArbitrageManager,
    runtime: ArbitrageTradeRuntime,
    *,
    derivative_inst_id: str = OPEN_DERIVATIVE_INST_ID,
) -> StepResult:
    try:
        opened = manager.open_now(make_open_request(Decimal("6"), "contracts", derivative_inst_id=derivative_inst_id), runtime=runtime)
        if not opened.success or not opened.ledger_entry_id:
            return StepResult("套利平仓：部分平仓", False, f"建测试仓失败：{opened.message}")
        entry = find_ledger_entry(opened.ledger_entry_id)
        if entry is None:
            return StepResult("套利平仓：部分平仓", False, "开仓成功但账本找不到记录。")
        partial_qty = snap_to_increment(entry.derivative_qty / Decimal("2"), Decimal("0.1"), "down")
        partial = manager.close_now(
            make_close_request(entry.entry_id, close_derivative_qty=partial_qty),
            runtime=runtime,
        )
        remaining_entry = find_ledger_entry(entry.entry_id)
        final_close = manager.close_now(make_close_request(entry.entry_id), runtime=runtime)
        return StepResult(
            "套利平仓：部分平仓",
            partial.success and final_close.success,
            "\n".join(
                [
                    f"entry_id={entry.entry_id}",
                    f"derivative_inst={derivative_inst_id}",
                    f"initial_derivative_qty={format_decimal(entry.derivative_qty)}",
                    f"partial_qty={format_decimal(partial_qty)}",
                    f"partial_message={partial.message}",
                    (
                        f"remaining_after_partial={format_decimal(remaining_entry.derivative_qty)}"
                        if remaining_entry is not None
                        else "remaining_after_partial=未找到剩余 open 记录"
                    ),
                    f"final_close_message={final_close.message}",
                ]
            ),
        )
    except Exception as exc:
        return StepResult("套利平仓：部分平仓", False, f"{exc}\n\n{traceback.format_exc()}")


def test_auto_open_close(
    manager: ArbitrageManager,
    client: OkxRestClient,
    runtime: ArbitrageTradeRuntime,
    *,
    derivative_inst_id: str = OPEN_DERIVATIVE_INST_ID,
) -> StepResult:
    try:
        spread_abs = current_spread_abs(client, derivative_inst_id)
        request = ArbitrageOpenRequest(
            base_ccy=BASE_CCY,
            spot_inst_id=SPOT_INST_ID,
            derivative_inst_id=derivative_inst_id,
            size=Decimal("2"),
            size_unit="contracts",
            trigger_mode="spread_abs",
            open_spread_pct_max=None,
            open_spread_abs_max=max(spread_abs - Decimal("10"), Decimal("1")),
            spot_limit_price=None,
            derivative_limit_price=None,
            use_limit_orders=False,
            max_slippage=MAX_SLIPPAGE,
            execution_mode="dual_taker",
            maker_wait_seconds=4.0,
            chase_limit=2,
        )
        manager.start_auto_open(request, runtime=runtime)
        if not wait_for(lambda: manager.auto_open.session is not None and manager.auto_open.session.result is not None, timeout_seconds=150):
            manager.stop_auto_open()
            return StepResult("套利脚本：自动开平仓", False, "自动开仓在超时内没有返回结果。")
        open_session = manager.auto_open.session
        assert open_session is not None
        open_result = open_session.result
        if open_result is None or not open_result.success or not open_result.ledger_entry_id:
            return StepResult("套利脚本：自动开平仓", False, f"自动开仓失败：{open_session.status}")
        manager.start_auto_close(
            request=make_close_request(open_result.ledger_entry_id),
            runtime=runtime,
            close_trigger_mode="spread_abs",
            close_spread_pct_min=None,
            close_spread_abs_min=spread_abs + Decimal("50"),
            entry_id=open_result.ledger_entry_id,
        )
        if not wait_for(lambda: manager.auto_close.session is not None and manager.auto_close.session.result is not None, timeout_seconds=150):
            manager.stop_auto_close()
            return StepResult("套利脚本：自动开平仓", False, "自动平仓在超时内没有返回结果。")
        close_session = manager.auto_close.session
        assert close_session is not None
        close_result = close_session.result
        success = open_result.success and close_result is not None and close_result.success
        return StepResult(
            "套利脚本：自动开平仓",
            success,
            "\n".join(
                [
                    f"derivative_inst={derivative_inst_id}",
                    f"spread_abs_at_start={format_decimal(spread_abs)}",
                    f"auto_open_status={open_session.status}",
                    f"open_message={open_result.message}",
                    summarize_entry(open_result.ledger_entry_id),
                    f"auto_close_status={close_session.status}",
                    f"close_message={close_result.message if close_result is not None else 'None'}",
                ]
            ),
        )
    except Exception as exc:
        return StepResult("套利脚本：自动开平仓", False, f"{exc}\n\n{traceback.format_exc()}")
    finally:
        try:
            manager.stop_auto_open()
        except Exception:
            pass
        try:
            manager.stop_auto_close()
        except Exception:
            pass


def _submit_market_order(
    client: OkxRestClient,
    runtime: ArbitrageTradeRuntime,
    *,
    inst_id: str,
    side: str,
    size: Decimal,
) -> None:
    if size <= 0:
        return
    config = _build_strategy_config(inst_id, runtime)
    order = client.place_simple_order(
        runtime.credentials,
        config,
        inst_id=inst_id,
        side=side,
        size=size,
        ord_type="market",
    )
    _wait_order_fill(
        client,
        credentials=runtime.credentials,
        config=config,
        inst_id=inst_id,
        ord_id=order.ord_id,
        expected_size=size,
        logger=lambda _message: None,
        label=f"cleanup {inst_id}",
    )


def cleanup_direct_positions(
    client: OkxRestClient,
    runtime: ArbitrageTradeRuntime,
    *,
    derivative_inst_ids: tuple[str, ...],
) -> None:
    positions = list(client.get_positions(runtime.credentials, environment=runtime.environment))
    positions.extend(account_spot_positions(client, runtime))
    for item in positions:
        if item.inst_id == SPOT_INST_ID and item.inst_type == "SPOT":
            if item.position > 0:
                try:
                    _submit_market_order(client, runtime, inst_id=SPOT_INST_ID, side="sell", size=item.position)
                except Exception:
                    pass
            elif item.position < 0:
                try:
                    _submit_market_order(client, runtime, inst_id=SPOT_INST_ID, side="buy", size=abs(item.position))
                except Exception:
                    pass
        if item.inst_id in derivative_inst_ids and item.inst_type in {"FUTURES", "SWAP"}:
            qty = abs(item.avail_position or item.position)
            if qty <= 0:
                continue
            side = "buy" if item.position < 0 else "sell"
            try:
                _submit_market_order(client, runtime, inst_id=item.inst_id, side=side, size=qty)
            except Exception:
                pass


def place_raw_pair(
    client: OkxRestClient,
    runtime: ArbitrageTradeRuntime,
    *,
    derivative_inst_id: str,
    contracts: Decimal,
) -> tuple[Decimal, Decimal]:
    spot_inst = client.get_instrument(SPOT_INST_ID)
    derivative_inst = client.get_instrument(derivative_inst_id)
    spot_ticker = client.get_ticker(SPOT_INST_ID)
    derivative_ticker = client.get_ticker(derivative_inst_id)
    spot_mid = mid_price(spot_ticker.bid, spot_ticker.ask)
    derivative_mid = mid_price(derivative_ticker.bid, derivative_ticker.ask)
    if spot_mid is None or derivative_mid is None:
        raise RuntimeError("无法计算参考价格。")
    spot_qty = _pair_spot_qty_from_derivative_qty(
        contracts,
        spot_instrument=spot_inst,
        derivative_instrument=derivative_inst,
        reference_price=derivative_mid,
    )
    _submit_market_order(client, runtime, inst_id=SPOT_INST_ID, side="buy", size=spot_qty)
    _submit_market_order(client, runtime, inst_id=derivative_inst_id, side="sell", size=contracts)
    return contracts, spot_qty


def build_pair_close_harness(client: OkxRestClient, runtime: ArbitrageTradeRuntime):
    harness = ArbitrageWindow.__new__(ArbitrageWindow)
    harness.client = client
    harness.manager = SimpleNamespace(
        auto_open=SimpleNamespace(is_running=False),
        auto_close=SimpleNamespace(is_running=False),
    )
    harness.window = _DummyWindow()
    harness._destroying = False
    harness._runtime_config_provider = None
    harness._runtime_or_warn = lambda: runtime
    harness._append_log_records = []
    harness._append_log = lambda message: harness._append_log_records.append(str(message))
    harness._refresh_pair_close_positions = lambda: None
    harness._pair_close_reference_prices = {}
    harness._pair_close_positions = []
    harness._pair_close_position_by_key = {}
    harness._pair_close_instruments = {}
    harness._pair_close_auto_thread = None
    harness._pair_close_auto_stop_event = threading.Event()
    harness._pair_close_auto_session = None
    harness.use_limit_orders = _Var(False)
    harness.max_slippage_percent = _Var("0.15")
    harness.pair_close_maker_wait_seconds = _Var("4")
    harness.pair_close_chase_limit = _Var("2")
    harness.pair_close_trigger_mode_label = _Var("绝对价差")
    harness.pair_close_spread_abs_min = _Var("999999")
    harness.pair_close_spread_pct_min = _Var("0")
    harness.pair_close_batch_count = _Var("2")
    harness.pair_close_batch_qty = _Var("")
    harness.pair_close_execution_mode_label = _Var("双腿吃单")
    harness.pair_close_status_text = _Var("")
    harness.pair_close_preview_text = _Var("")
    harness.pair_close_derivative_qty = _Var("")
    harness._pair_close_spot_key = _Var("")
    harness._pair_close_derivative_key = _Var("")
    harness.status_text = _Var("")
    harness._api_profile_names = [PROFILE_NAME]
    harness.api_profile_name = _Var(PROFILE_NAME)
    harness._last_api_profile_name = PROFILE_NAME
    return harness


def attach_live_pair_to_harness(
    harness,
    client: OkxRestClient,
    runtime: ArbitrageTradeRuntime,
    *,
    derivative_inst_id: str,
    derivative_qty: Decimal,
) -> tuple[str, str]:
    positions = list(client.get_positions(runtime.credentials, environment=runtime.environment))
    positions.extend(account_spot_positions(client, runtime))
    spot_positions = [item for item in positions if item.inst_id == SPOT_INST_ID and item.inst_type == "SPOT" and item.position > 0]
    derivative_positions = [
        item
        for item in positions
        if item.inst_id == derivative_inst_id and item.inst_type in {"FUTURES", "SWAP"} and item.position < 0
    ]
    if not spot_positions or not derivative_positions:
        raise RuntimeError("没有找到可用于配对平仓测试的现货/合约持仓。")
    spot_position = spot_positions[0]
    derivative_position = derivative_positions[0]
    harness._pair_close_positions = [spot_position, derivative_position]
    harness._pair_close_instruments = {
        SPOT_INST_ID: client.get_instrument(SPOT_INST_ID),
        derivative_inst_id: client.get_instrument(derivative_inst_id),
    }
    spot_label = "spot-test"
    derivative_label = "derivative-test"
    harness._pair_close_position_by_key = {
        spot_label: spot_position,
        derivative_label: derivative_position,
    }
    harness._pair_close_spot_key.set(spot_label)
    harness._pair_close_derivative_key.set(derivative_label)
    harness.pair_close_derivative_qty.set(format_decimal(derivative_qty))
    return _pair_position_direction(spot_position), _pair_position_direction(derivative_position)


def positions_for_instruments(
    client: OkxRestClient,
    runtime: ArbitrageTradeRuntime,
    *,
    derivative_inst_ids: tuple[str, ...],
) -> list[str]:
    positions = list(client.get_positions(runtime.credentials, environment=runtime.environment))
    rows = []
    for item in positions:
        if item.inst_id in set(derivative_inst_ids):
            rows.append(f"{item.inst_id} | pos={item.position} | avail={item.avail_position} | side={item.pos_side}")
    for item in account_spot_positions(client, runtime):
        if item.inst_id == SPOT_INST_ID:
            rows.append(f"{item.inst_id} | pos={item.position} | avail={item.avail_position} | side={item.pos_side}")
    return rows


def test_pair_close_manual(
    client: OkxRestClient,
    runtime: ArbitrageTradeRuntime,
    *,
    execution_label: str,
    execution_mode: str,
    derivative_inst_id: str = PAIR_CLOSE_DERIVATIVE_INST_ID,
) -> StepResult:
    try:
        cleanup_direct_positions(client, runtime, derivative_inst_ids=(OPEN_DERIVATIVE_INST_ID, PAIR_CLOSE_DERIVATIVE_INST_ID))
        contracts = Decimal("4")
        place_raw_pair(client, runtime, derivative_inst_id=derivative_inst_id, contracts=contracts)
        harness = build_pair_close_harness(client, runtime)
        harness.pair_close_execution_mode_label.set(execution_label)
        spot_direction, derivative_direction = attach_live_pair_to_harness(
            harness,
            client,
            runtime,
            derivative_inst_id=derivative_inst_id,
            derivative_qty=contracts,
        )
        message = harness._execute_pair_close_batches(  # noqa: SLF001
            runtime,
            spot_inst_id=SPOT_INST_ID,
            derivative_inst_id=derivative_inst_id,
            spot_direction=spot_direction,
            derivative_direction=derivative_direction,
            total_derivative_qty=contracts,
            planned_batches=[Decimal("2"), Decimal("2")],
            execution_mode=execution_mode,
        )
        remaining = positions_for_instruments(client, runtime, derivative_inst_ids=(derivative_inst_id,))
        return StepResult(
            f"持仓配对平仓：手动批次执行 | {execution_label}",
            len(remaining) == 0,
            "\n".join(
                [
                    f"open_contracts={format_decimal(contracts)}",
                    f"derivative_inst={derivative_inst_id}",
                    message,
                    "remaining_positions=" + ("无" if not remaining else "; ".join(remaining)),
                ]
            ),
        )
    except Exception as exc:
        return StepResult(f"持仓配对平仓：手动批次执行 | {execution_label}", False, f"{exc}\n\n{traceback.format_exc()}")
    finally:
        cleanup_direct_positions(client, runtime, derivative_inst_ids=(OPEN_DERIVATIVE_INST_ID, PAIR_CLOSE_DERIVATIVE_INST_ID))


def test_pair_close_auto(
    client: OkxRestClient,
    runtime: ArbitrageTradeRuntime,
    *,
    derivative_inst_id: str = PAIR_CLOSE_DERIVATIVE_INST_ID,
) -> StepResult:
    try:
        cleanup_direct_positions(client, runtime, derivative_inst_ids=(OPEN_DERIVATIVE_INST_ID, PAIR_CLOSE_DERIVATIVE_INST_ID))
        contracts = Decimal("4")
        place_raw_pair(client, runtime, derivative_inst_id=derivative_inst_id, contracts=contracts)
        harness = build_pair_close_harness(client, runtime)
        attach_live_pair_to_harness(
            harness,
            client,
            runtime,
            derivative_inst_id=derivative_inst_id,
            derivative_qty=contracts,
        )
        harness.pair_close_spread_abs_min.set(format_decimal(current_spread_abs(client, derivative_inst_id) + Decimal("20")))
        harness._start_pair_close_auto()  # noqa: SLF001
        if not wait_for(lambda: not harness._is_pair_close_auto_running(), timeout_seconds=150, interval_seconds=1.0):  # noqa: SLF001
            harness._stop_pair_close_auto(silent=True)  # noqa: SLF001
            return StepResult("持仓配对平仓：自动执行", False, "自动配对平仓线程超时未结束。")
        remaining = positions_for_instruments(client, runtime, derivative_inst_ids=(derivative_inst_id,))
        return StepResult(
            "持仓配对平仓：自动执行",
            len(remaining) == 0,
            "\n".join(
                [
                    f"derivative_inst={derivative_inst_id}",
                    f"status={harness.pair_close_status_text.get()}",
                    "logs=",
                    *harness._append_log_records,
                    "remaining_positions=" + ("无" if not remaining else "; ".join(remaining)),
                ]
            ),
        )
    except Exception as exc:
        return StepResult("持仓配对平仓：自动执行", False, f"{exc}\n\n{traceback.format_exc()}")
    finally:
        cleanup_direct_positions(client, runtime, derivative_inst_ids=(OPEN_DERIVATIVE_INST_ID, PAIR_CLOSE_DERIVATIVE_INST_ID))


def cleanup_open_ledger(manager: ArbitrageManager, runtime: ArbitrageTradeRuntime) -> None:
    for entry in load_open_ledger_entries():
        if entry.base_ccy != BASE_CCY or entry.derivative_inst_id not in {OPEN_DERIVATIVE_INST_ID, PAIR_CLOSE_DERIVATIVE_INST_ID}:
            continue
        try:
            manager.close_now(make_close_request(entry.entry_id), runtime=runtime)
        except Exception:
            pass


def execution_mode_rows(manager: ArbitrageManager, runtime: ArbitrageTradeRuntime) -> list[StepResult]:
    rows: list[StepResult] = []
    for label, mode in EXECUTION_MODES:
        rows.append(
            open_and_close_manual(
                manager,
                runtime,
                title=f"执行方式={label}",
                size=Decimal("2"),
                unit="contracts",
                derivative_inst_id=OPEN_DERIVATIVE_INST_ID,
                open_execution_mode=mode,
                close_execution_mode=mode,
            )
        )
    return rows


def main() -> int:
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report = HtmlReport()
    client, manager, runtime = load_runtime()
    cleanup_open_ledger(manager, runtime)
    cleanup_direct_positions(client, runtime, derivative_inst_ids=(OPEN_DERIVATIVE_INST_ID, PAIR_CLOSE_DERIVATIVE_INST_ID))
    try:
        initial_positions, initial_labels = position_snapshot(client, runtime)
        snapshot_lines = [f"open_positions_count={len(initial_positions)}"]
        snapshot_lines.extend(initial_labels if initial_labels else ["当前没有衍生品持仓。"])
        snapshot_lines.append(f"current_spread_abs_open={format_decimal(current_spread_abs(client, OPEN_DERIVATIVE_INST_ID))}")
        snapshot_lines.append(f"current_spread_abs_pair_close={format_decimal(current_spread_abs(client, PAIR_CLOSE_DERIVATIVE_INST_ID))}")
        report.add_section(
            "测试前状态",
            [
                StepResult(
                    "moni/demo 当前账户快照",
                    True,
                    "\n".join(snapshot_lines),
                )
            ],
        )

        scan_rows = manager.scan_opportunities(include_swap=True, include_futures=True)
        report.add_section(
            "机会扫描",
            [
                StepResult(
                    "套利机会扫描",
                    len(scan_rows) > 0,
                    "\n".join(
                        [f"机会数量={len(scan_rows)}"]
                        + [
                            f"{item.base_ccy} | {item.pair_kind_label} | {item.spot_inst_id} | {item.derivative_inst_id} | abs={format_decimal(item.basis_abs)} | net={format_decimal(item.net_annual_pct)}%"
                            for item in scan_rows[:10]
                        ]
                    ),
                )
            ],
        )

        report.add_section(
            "套利开仓：三种数量单位",
            [
                open_and_close_manual(manager, runtime, title="投入数量=币数", size=Decimal("0.002"), unit="coin"),
                open_and_close_manual(manager, runtime, title="投入数量=USDT金额", size=Decimal("150"), unit="usdt"),
                open_and_close_manual(manager, runtime, title="投入数量=合约张数", size=Decimal("3"), unit="contracts"),
            ],
        )

        report.add_section(
            "套利开平仓：三种执行方式",
            execution_mode_rows(manager, runtime),
        )

        report.add_section(
            "套利平仓",
            [
                test_manual_partial_close(manager, runtime),
            ],
        )

        report.add_section(
            "套利脚本",
            [
                test_auto_open_close(manager, client, runtime),
            ],
        )

        report.add_section(
            "持仓配对平仓",
            [
                *[
                    test_pair_close_manual(client, runtime, execution_label=label, execution_mode=mode)
                    for label, mode in EXECUTION_MODES
                ],
                test_pair_close_auto(client, runtime),
            ],
        )
    finally:
        cleanup_open_ledger(manager, runtime)
        cleanup_direct_positions(client, runtime, derivative_inst_ids=(OPEN_DERIVATIVE_INST_ID, PAIR_CLOSE_DERIVATIVE_INST_ID))
    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    report.save(REPORT_PATH, started_at=started_at, finished_at=finished_at)
    print(REPORT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
