#!/usr/bin/env python3
"""
诊断 OKX 行情接口返回的触发价字段（与策略引擎 / 动态止盈接管读取路径一致）。

用法（在项目根目录）:
  python scripts/diagnose_okx_trigger_price.py
  python scripts/diagnose_okx_trigger_price.py --inst BTC-USDT-SWAP --repeat 3

说明:
  - 使用公开 REST，无需 API Key。
  - 打印 `/api/v5/market/ticker` 单条里的 last / markPx / idxPx 等原始键。
  - 对比 `OkxRestClient.get_trigger_price`（mark 会在 ticker 缺 mark 时回退 `get_mark_price`）。
  - 若 ticker 中 markPx 为空而 mark-price 接口有值，则历史上 `EngineRetryPolicy` 只读 ticker
    会误报「OKX 未返回有效触发价」；引擎已改为委托客户端统一逻辑。
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from decimal import Decimal


def _main() -> int:
    parser = argparse.ArgumentParser(description="诊断 OKX 触发价（ticker vs mark-price vs get_trigger_price）")
    parser.add_argument("--inst", default="ETH-USDT-SWAP", help="合约 instId，默认 ETH-USDT-SWAP")
    parser.add_argument("--repeat", type=int, default=1, help="连续请求次数，观察是否偶发空字段")
    args = parser.parse_args()
    inst = args.inst.strip().upper()

    repo_root = __file__.rsplit("scripts", 1)[0].rstrip("/\\")
    if repo_root and repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    from okx_quant.okx_client import OkxApiError, OkxRestClient

    client = OkxRestClient()

    print(f"标的: {inst} | 连续请求: {args.repeat}")
    print("-" * 72)

    for i in range(1, args.repeat + 1):
        if i > 1:
            time.sleep(0.35)
        print(f"\n--- 第 {i}/{args.repeat} 次 ---")
        try:
            ticker = client.get_ticker(inst)
        except OkxApiError as e:
            print(f"get_ticker 失败: {e}")
            continue
        raw = ticker.raw if isinstance(getattr(ticker, "raw", None), dict) else {}
        keys_of_interest = ("last", "bidPx", "askPx", "markPx", "idxPx", "indexPx", "ts", "sodUtc8", "instType")
        snap = {k: raw.get(k) for k in keys_of_interest if k in raw}
        print("ticker 摘要字段:", json.dumps(snap, ensure_ascii=False, default=str))
        print(
            f"解析后: last={ticker.last!s} mark={ticker.mark!s} bid={ticker.bid!s} "
            f"ask={ticker.ask!s} index={ticker.index!s}"
        )

        try:
            mp = client.get_mark_price(inst)
            print(f"get_mark_price(public): {mp}")
        except OkxApiError as e:
            print(f"get_mark_price 失败: {e}")

        for typ in ("mark", "last", "index"):
            try:
                px = client.get_trigger_price(inst, typ)  # type: ignore[arg-type]
                print(f"get_trigger_price({typ!r}): {px}")
            except OkxApiError as e:
                print(f"get_trigger_price({typ!r}) 失败: {e}")

    print("\n" + "-" * 72)
    print("若 ticker.markPx 常为空而 get_mark_price 正常，说明应走客户端 get_trigger_price 的 mark 回退；")
    print("引擎侧 `EngineRetryPolicy.get_trigger_price` 已与客户端对齐。")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
