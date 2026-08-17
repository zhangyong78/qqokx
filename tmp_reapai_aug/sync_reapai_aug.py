"""只读同步 ReapAI 账户 2026-08 交易历史，用于离线分析。

仅发起 GET 查询（历史仓位 / 历史成交 / 当前持仓 / 账户余额 / 历史委托），
不下单、不撤单、不修改任何账户状态；也不写应用自身的缓存文件，
原始数据另存到 tmp_reapai_aug/ 目录供分析使用。
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from okx_quant.okx_client import OkxRestClient
from okx_quant.models import Credentials
from okx_quant.persistence import load_credentials_snapshot

BJT = timezone(timedelta(hours=8))
OUT_DIR = PROJECT_ROOT / "tmp_reapai_aug"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PROFILE = "ReapAI"

AUG_BEGIN_MS = int(datetime(2026, 8, 1, 0, 0, tzinfo=BJT).timestamp() * 1000)
AUG_END_MS = int((datetime.now(BJT) + timedelta(minutes=1)).timestamp() * 1000)


def main() -> None:
    profile = load_credentials_snapshot(profile_name=PROFILE)
    api_key = profile.get("api_key", "")
    secret_key = profile.get("secret_key", "")
    passphrase = profile.get("passphrase", "")
    environment = (profile.get("environment", "") or "").strip() or "live"
    if not api_key or not secret_key:
        print("ERROR: ReapAI profile 凭据为空或解密失败")
        sys.exit(1)
    creds = Credentials(api_key=api_key, secret_key=secret_key, passphrase=passphrase, profile_name=PROFILE)
    client = OkxRestClient()
    simulated = environment == "demo"
    print(f"profile={PROFILE} environment={environment} simulated={simulated}")
    print(f"window: {datetime.fromtimestamp(AUG_BEGIN_MS/1000, BJT)} ~ {datetime.fromtimestamp(AUG_END_MS/1000, BJT)}")

    result: dict = {"profile": PROFILE, "environment": environment, "fetched_at": datetime.now(BJT).isoformat()}

    # 1) 历史仓位（已平仓），limit 1000 一次取全
    pos_payload = client._request(
        "GET", "/api/v5/account/positions-history",
        params={"instType": "SWAP", "limit": "1000"},
        auth=True, credentials=creds, simulated=simulated,
    )
    pos_records = pos_payload.get("data", [])
    result["positions_history_swap"] = pos_records
    print(f"positions-history SWAP: {len(pos_records)} 条")

    # 2) 历史成交：按 begin/end 窗口分页拉取（3 个月内有效）
    fills: list[dict] = []
    after: str | None = None
    for _ in range(60):  # 最多 60 页 x 100 = 6000 笔
        params = {"instType": "SWAP", "limit": "100", "begin": str(AUG_BEGIN_MS), "end": str(AUG_END_MS)}
        if after:
            params["after"] = after
        payload = client._request(
            "GET", "/api/v5/trade/fills-history",
            params=params, auth=True, credentials=creds, simulated=simulated,
        )
        batch = payload.get("data", [])
        if not batch:
            break
        fills.extend(batch)
        after = str(batch[-1].get("billId") or batch[-1].get("ts") or "")
        if not after or len(batch) < 100:
            break
        time.sleep(0.25)
    result["fills_august"] = fills
    print(f"fills-history 8月窗口: {len(fills)} 笔")

    # 3) 当前持仓
    positions_payload = client._request(
        "GET", "/api/v5/account/positions", params={"instType": "SWAP"},
        auth=True, credentials=creds, simulated=simulated,
    )
    result["current_positions"] = positions_payload.get("data", [])
    print(f"current positions: {len(result['current_positions'])} 个")

    # 4) 账户余额
    balance_payload = client._request(
        "GET", "/api/v5/account/balance", params={"ccy": "USDT"},
        auth=True, credentials=creds, simulated=simulated,
    )
    result["balance"] = balance_payload.get("data", [])
    if result["balance"]:
        det = result["balance"][0].get("details", [{}])[0]
        print(f"equity={det.get('eq')} availEq={det.get('availEq')} upl={det.get('upl')}")

    # 5) 历史委托（用于 clOrdId 策略归因），分页拉 8 月窗口
    orders: list[dict] = []
    after = None
    for _ in range(60):
        params = {"instType": "SWAP", "limit": "100", "begin": str(AUG_BEGIN_MS), "end": str(AUG_END_MS)}
        if after:
            params["after"] = after
        payload = client._request(
            "GET", "/api/v5/trade/orders-history",
            params=params, auth=True, credentials=creds, simulated=simulated,
        )
        batch = payload.get("data", [])
        if not batch:
            break
        orders.extend(batch)
        after = str(batch[-1].get("ordId") or "")
        if not after or len(batch) < 100:
            break
        time.sleep(0.25)
    result["orders_august"] = orders
    print(f"orders-history 8月窗口: {len(orders)} 条")

    out_path = OUT_DIR / "reapai_august_raw.json"
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"saved -> {out_path}")


if __name__ == "__main__":
    main()
