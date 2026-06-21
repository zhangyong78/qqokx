from __future__ import annotations

from roll_terminal_qt.models import ArbitrageOpportunityView


def default_opportunities() -> list[ArbitrageOpportunityView]:
    return [
        ArbitrageOpportunityView(
            key="btc_roll_near_next",
            title="BTC 交割换月",
            left_inst_id="BTC-USD-260626",
            right_inst_id="BTC-USD-260925",
            left_kind="交割",
            right_kind="交割",
            template="roll",
            description="当前交割 -> 远月交割，保留现货腿的专业移仓模板。",
        ),
        ArbitrageOpportunityView(
            key="btc_cash_futures",
            title="BTC 期现套利",
            left_inst_id="BTC-USD-260626",
            right_inst_id="BTC-USDT",
            left_kind="交割",
            right_kind="现货",
            template="professional",
            description="交割/现货双腿看盘与后续专业套利下单入口。",
        ),
        ArbitrageOpportunityView(
            key="btc_perp_spot",
            title="BTC 永续期现",
            left_inst_id="BTC-USDT-SWAP",
            right_inst_id="BTC-USDT",
            left_kind="永续",
            right_kind="现货",
            template="professional",
            description="永续/现货价差观察与双腿执行入口。",
        ),
        ArbitrageOpportunityView(
            key="eth_perp_spot",
            title="ETH 永续期现",
            left_inst_id="ETH-USDT-SWAP",
            right_inst_id="ETH-USDT",
            left_kind="永续",
            right_kind="现货",
            template="professional",
            description="ETH 永续与现货的专业套利模板。",
        ),
        ArbitrageOpportunityView(
            key="eth_roll_quarter",
            title="ETH 交割换月",
            left_inst_id="ETH-USD-260626",
            right_inst_id="ETH-USD-260925",
            left_kind="交割",
            right_kind="交割",
            template="roll",
            description="ETH 交割合约换月模板。",
        ),
    ]


def filter_opportunities(items: list[ArbitrageOpportunityView], keyword: str) -> list[ArbitrageOpportunityView]:
    query = keyword.strip().lower()
    if not query:
        return list(items)
    result: list[ArbitrageOpportunityView] = []
    for item in items:
        haystack = " | ".join(
            (
                item.title,
                item.left_inst_id,
                item.right_inst_id,
                item.left_kind,
                item.right_kind,
                item.template,
                item.description,
            )
        ).lower()
        if query in haystack:
            result.append(item)
    return result
