#!/usr/bin/env python3
"""End-to-end arbitrage demo test on OKX simulated trading (moni profile)."""
from __future__ import annotations

import json
import sys
import time
import traceback
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from okx_quant.arbitrage.arbitrage_executor import ArbitrageOpenRequest
from okx_quant.arbitrage.arbitrage_manager import ArbitrageManager
from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.models import Credentials
from okx_quant.okx_client import OkxApiError, OkxRestClient
from okx_quant.persistence import load_credentials_snapshot
from okx_quant.pricing import format_decimal, format_decimal_fixed


REPORT_PATH = Path(__file__).resolve().parents[1] / "reports" / "arbitrage_demo_test_report.json"
PROFILE = "moni"
BASE = "BTC"
DERIV = "BTC-USDT-SWAP"
TEST_USDT = Decimal("50")


@dataclass
class StepResult:
    name: str
    ok: bool
    detail: str = ""
    data: dict = field(default_factory=dict)


def _load_runtime() -> tuple[Credentials, ArbitrageTradeRuntime]:
    snap = load_credentials_snapshot(profile_name=PROFILE)
    creds = Credentials(
        api_key=snap["api_key"],
        secret_key=snap["secret_key"],
        passphrase=snap["passphrase"],
        profile_name=PROFILE,
    )
    runtime = ArbitrageTradeRuntime(
        credentials=creds,
        environment="demo",
        trade_mode="cross",
        position_mode="net",
        credential_profile_name=PROFILE,
    )
    return creds, runtime


def _run_steps() -> list[StepResult]:
    results: list[StepResult] = []
    logs: list[str] = []

    def log(msg: str) -> None:
        logs.append(msg)
        print(msg, flush=True)

    try:
        creds, runtime = _load_runtime()
        results.append(StepResult("load_credentials", True, f"profile={PROFILE} env=demo"))
    except Exception as exc:
        results.append(StepResult("load_credentials", False, str(exc)))
        return results

    client = OkxRestClient()
    manager = ArbitrageManager(client, logger=log)

    # 1) Account connectivity
    try:
        overview = client.get_account_overview(creds, environment="demo")
        eq = overview.total_equity or overview.available_equity or Decimal("0")
        results.append(
            StepResult(
                "account_overview",
                True,
                f"total_equity={format_decimal_fixed(eq, 4)}",
                {"total_equity": str(eq)},
            )
        )
    except Exception as exc:
        results.append(StepResult("account_overview", False, str(exc)))
        return results

    # 2) Scanner
    try:
        rows = manager.scan_opportunities()
        btc_rows = [r for r in rows if r.base_ccy == BASE and r.derivative_inst_id == DERIV]
        top = btc_rows[0] if btc_rows else (rows[0] if rows else None)
        if top is None:
            results.append(StepResult("scanner", False, "no opportunities"))
        else:
            results.append(
                StepResult(
                    "scanner",
                    True,
                    f"total={len(rows)} btc_spread={format_decimal_fixed(top.basis_pct, 4)}% net={format_decimal_fixed(top.net_annual_pct, 2)}%",
                    {
                        "count": len(rows),
                        "btc_basis_pct": str(top.basis_pct),
                        "btc_net_annual_pct": str(top.net_annual_pct),
                    },
                )
            )
    except Exception as exc:
        results.append(StepResult("scanner", False, str(exc)))
        traceback.print_exc()

    # 3) Size preview
    try:
        preview = manager.preview_size(
            base_ccy=BASE,
            derivative_inst_id=DERIV,
            size=TEST_USDT,
            unit="usdt",
        )
        results.append(
            StepResult(
                "size_preview",
                True,
                f"spot={format_decimal(preview.spot_base_qty)} swap={format_decimal(preview.swap_contracts)} notional={format_decimal_fixed(preview.notional_usdt, 2)}",
                {
                    "spot_qty": str(preview.spot_base_qty),
                    "swap_contracts": str(preview.swap_contracts),
                },
            )
        )
    except Exception as exc:
        results.append(StepResult("size_preview", False, str(exc)))

    # 4) Immediate open (small USDT)
    open_result = None
    try:
        request = ArbitrageOpenRequest(
            base_ccy=BASE,
            spot_inst_id=f"{BASE}-USDT",
            derivative_inst_id=DERIV,
            size=TEST_USDT,
            size_unit="usdt",
            trigger_mode="spread",
            open_spread_pct_max=Decimal("999"),
            spot_limit_price=None,
            derivative_limit_price=None,
            use_limit_orders=False,
            max_slippage=Decimal("0.005"),
        )
        log(f"--- immediate open {TEST_USDT} USDT ---")
        open_result = manager.open_now(request, runtime=runtime)
        results.append(
            StepResult(
                "immediate_open",
                open_result.success,
                open_result.message,
                {
                    "spot_filled": str(open_result.spot_filled_qty),
                    "deriv_filled": str(open_result.derivative_filled_qty),
                    "ledger_id": open_result.ledger_entry_id or "",
                },
            )
        )
    except Exception as exc:
        results.append(StepResult("immediate_open", False, str(exc)))
        traceback.print_exc()

    # 5) Verify ledger
    try:
        entries = manager.load_ledger()
        latest = entries[0] if entries else None
        results.append(
            StepResult(
                "ledger",
                latest is not None and latest.close_mode == "open",
                f"entries={len(entries)} latest={latest.base_ccy if latest else '-'}",
                {"entry_count": len(entries)},
            )
        )
    except Exception as exc:
        results.append(StepResult("ledger", False, str(exc)))

    # 6) Auto-open monitor (trigger with very loose spread, should fire quickly)
    try:
        spot_t = client.get_ticker(f"{BASE}-USDT")
        deriv_t = client.get_ticker(DERIV)
        from okx_quant.arbitrage.basis_calculator import mid_price

        sm = mid_price(spot_t.bid, spot_t.ask)
        dm = mid_price(deriv_t.bid, deriv_t.ask)
        spread = (dm - sm) / sm * Decimal("100") if sm and dm and sm > 0 else Decimal("0")
        trigger_spread = spread + Decimal("1")  # always above current => triggers on next poll

        auto_req = ArbitrageOpenRequest(
            base_ccy=BASE,
            spot_inst_id=f"{BASE}-USDT",
            derivative_inst_id=DERIV,
            size=Decimal("30"),
            size_unit="usdt",
            trigger_mode="spread",
            open_spread_pct_max=trigger_spread,
            spot_limit_price=None,
            derivative_limit_price=None,
            use_limit_orders=False,
            max_slippage=Decimal("0.005"),
        )
        log(f"--- auto-open monitor trigger_spread={trigger_spread:.4f}% current={spread:.4f}% ---")
        manager.start_auto_open(auto_req, runtime=runtime)
        deadline = time.time() + 30
        session_status = "timeout"
        while time.time() < deadline:
            session = manager.auto_open.session
            if session is not None and session.triggered:
                session_status = session.status
                break
            time.sleep(0.5)
        manager.stop_auto_open()
        session = manager.auto_open.session
        auto_ok = session is not None and session.result is not None and session.result.success
        results.append(
            StepResult(
                "auto_open_monitor",
                auto_ok,
                session_status if session else "no session",
                {
                    "trigger_spread_pct": str(trigger_spread),
                    "current_spread_pct": str(spread),
                    "result": session.result.message if session and session.result else "",
                },
            )
        )
    except Exception as exc:
        results.append(StepResult("auto_open_monitor", False, str(exc)))
        traceback.print_exc()
        try:
            manager.stop_auto_open()
        except Exception:
            pass

    # 7) Positions check
    try:
        positions = client.get_positions(creds, environment="demo")
        relevant = [p for p in positions if BASE in p.inst_id]
        results.append(
            StepResult(
                "positions_after",
                True,
                f"relevant_positions={len(relevant)}",
                {
                    "positions": [
                        {
                            "inst_id": p.inst_id,
                            "pos": str(p.position),
                            "side": p.pos_side,
                        }
                        for p in relevant[:6]
                    ]
                },
            )
        )
    except Exception as exc:
        results.append(StepResult("positions_after", False, str(exc)))

    return results


def main() -> int:
    print(f"=== Arbitrage Demo E2E | profile={PROFILE} ===", flush=True)
    results = _run_steps()
    passed = sum(1 for r in results if r.ok)
    total = len(results)
    all_ok = passed == total and total > 0

    report = {
        "profile": PROFILE,
        "environment": "demo",
        "all_passed": all_ok,
        "passed": passed,
        "total": total,
        "steps": [
            {"name": r.name, "ok": r.ok, "detail": r.detail, "data": r.data}
            for r in results
        ],
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n=== SUMMARY ===", flush=True)
    for r in results:
        mark = "PASS" if r.ok else "FAIL"
        print(f"[{mark}] {r.name}: {r.detail}", flush=True)
    print(f"\nResult: {passed}/{total} passed | report={REPORT_PATH}", flush=True)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
