from __future__ import annotations

import re
from datetime import date, datetime, timezone
from decimal import Decimal

from okx_quant.arbitrage.basis_calculator import (
    annualize_funding_rate,
    compute_basis,
    mid_price,
    net_carry_annual_pct_cash_and_carry,
)
from okx_quant.arbitrage.fee_calculator import round_trip_fee_pct
from okx_quant.arbitrage.models import ArbitrageOpportunity, ArbitragePairKind, ArbitrageRuntimeConfig
from okx_quant.arbitrage.order_book_analyzer import spread_slippage_proxy
from okx_quant.models import Instrument
from okx_quant.okx_client import OkxRestClient, OkxTicker, OkxApiError

_PAIR_KIND_LABELS = {
    "spot_swap": "现货+永续",
    "spot_quarter": "现货+当季",
    "spot_next_quarter": "现货+次季",
}
_FUTURES_EXPIRY_PATTERN = re.compile(r"^([A-Z0-9]+)-([A-Z0-9]+)-(\d{6})$")


def _parse_futures_expiry(inst_id: str) -> date | None:
    match = _FUTURES_EXPIRY_PATTERN.match(inst_id.strip().upper())
    if not match:
        return None
    yymmdd = match.group(3)
    try:
        return datetime.strptime(yymmdd, "%y%m%d").date()
    except ValueError:
        return None


def _days_to_expiry(expiry: date, *, today: date | None = None) -> int:
    ref = today or datetime.now(timezone.utc).date()
    return max((expiry - ref).days, 1)


def _ticker_funding_rate(ticker: OkxTicker) -> Decimal | None:
    raw = ticker.raw if isinstance(getattr(ticker, "raw", None), dict) else {}
    value = raw.get("fundingRate")
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _spot_usdt_bases(spot_instruments: list[Instrument]) -> dict[str, Instrument]:
    mapping: dict[str, Instrument] = {}
    for item in spot_instruments:
        if item.state.lower() != "live":
            continue
        parts = item.inst_id.split("-")
        if len(parts) != 2 or parts[1] != "USDT":
            continue
        mapping[parts[0]] = item
    return mapping


def _swap_by_base(swap_instruments: list[Instrument]) -> dict[str, Instrument]:
    mapping: dict[str, Instrument] = {}
    for item in swap_instruments:
        if item.state.lower() != "live":
            continue
        if not item.inst_id.endswith("-USDT-SWAP"):
            continue
        base = item.inst_id.split("-")[0]
        mapping[base] = item
    return mapping


def _futures_quarter_pairs(futures_instruments: list[Instrument]) -> dict[str, tuple[Instrument, Instrument | None]]:
    grouped: dict[str, list[tuple[date, Instrument]]] = {}
    today = datetime.now(timezone.utc).date()
    for item in futures_instruments:
        if item.state.lower() != "live":
            continue
        if item.settle_ccy not in {None, "USDT"}:
            continue
        expiry = _parse_futures_expiry(item.inst_id)
        if expiry is None or expiry <= today:
            continue
        base = item.inst_id.split("-")[0]
        grouped.setdefault(base, []).append((expiry, item))
    result: dict[str, tuple[Instrument, Instrument | None]] = {}
    for base, rows in grouped.items():
        rows.sort(key=lambda pair: pair[0])
        quarter = rows[0][1]
        next_quarter = rows[1][1] if len(rows) > 1 else None
        result[base] = (quarter, next_quarter)
    return result


def _build_opportunity(
    *,
    base_ccy: str,
    pair_kind: ArbitragePairKind,
    spot_inst_id: str,
    derivative_inst_id: str,
    spot_ticker: OkxTicker,
    derivative_ticker: OkxTicker,
    config: ArbitrageRuntimeConfig,
    days_to_expiry: int | None,
) -> ArbitrageOpportunity | None:
    spot_mid = mid_price(spot_ticker.bid, spot_ticker.ask)
    derivative_mid = mid_price(derivative_ticker.bid, derivative_ticker.ask)
    if spot_mid is None or derivative_mid is None:
        return None
    _, basis_pct = compute_basis(spot_mid, derivative_mid)
    funding_rate = _ticker_funding_rate(derivative_ticker)
    funding_annual = (
        annualize_funding_rate(
            funding_rate,
            funding_intervals_per_day=config.funding_intervals_per_day,
        )
        if funding_rate is not None
        else Decimal("0")
    )
    fee_pct = round_trip_fee_pct(fee_profile=config.fee_profile, assume_taker=True)
    slippage_pct = spread_slippage_proxy(spot_ticker.bid, spot_ticker.ask) + spread_slippage_proxy(
        derivative_ticker.bid,
        derivative_ticker.ask,
    )
    hold_days = Decimal(str(days_to_expiry)) if days_to_expiry is not None else None
    net_annual = net_carry_annual_pct_cash_and_carry(
        basis_pct=basis_pct,
        funding_annual=funding_annual,
        fee_round_trip_pct=fee_pct,
        slippage_pct=slippage_pct,
        hold_days=hold_days,
    )
    funding_annual_pct = funding_annual * Decimal("100") if funding_rate is not None else None
    return ArbitrageOpportunity(
        base_ccy=base_ccy,
        pair_kind=pair_kind,
        pair_kind_label=_PAIR_KIND_LABELS[pair_kind],
        spot_inst_id=spot_inst_id,
        derivative_inst_id=derivative_inst_id,
        spot_mid=spot_mid,
        derivative_mid=derivative_mid,
        basis_abs=derivative_mid - spot_mid,
        basis_pct=basis_pct * Decimal("100"),
        funding_rate=funding_rate,
        funding_annual_pct=funding_annual_pct,
        fee_round_trip_pct=fee_pct * Decimal("100"),
        slippage_est_pct=slippage_pct * Decimal("100"),
        net_annual_pct=net_annual,
        days_to_expiry=days_to_expiry,
        scanned_at=datetime.now(timezone.utc),
    )


class ArbitrageScanner:
    def __init__(self, client: OkxRestClient, *, config: ArbitrageRuntimeConfig | None = None) -> None:
        self._client = client
        self._config = config or ArbitrageRuntimeConfig()

    @property
    def config(self) -> ArbitrageRuntimeConfig:
        return self._config

    def scan(self) -> list[ArbitrageOpportunity]:
        try:
            spot_instruments = self._client.get_spot_instruments()
            swap_instruments = self._client.get_swap_instruments()
            futures_instruments = self._client.get_instruments("FUTURES")
        except OkxApiError:
            raise

        spot_bases = _spot_usdt_bases(spot_instruments)
        swap_map = _swap_by_base(swap_instruments)
        futures_map = _futures_quarter_pairs(futures_instruments)

        spot_tickers = {item.inst_id: item for item in self._client.get_tickers("SPOT")}
        swap_tickers = {item.inst_id: item for item in self._client.get_tickers("SWAP")}
        futures_tickers = {item.inst_id: item for item in self._client.get_tickers("FUTURES")}

        opportunities: list[ArbitrageOpportunity] = []
        for base, spot_inst in spot_bases.items():
            spot_ticker = spot_tickers.get(spot_inst.inst_id)
            if spot_ticker is None:
                continue

            swap_inst = swap_map.get(base)
            if swap_inst is not None:
                swap_ticker = swap_tickers.get(swap_inst.inst_id)
                if swap_ticker is not None:
                    row = _build_opportunity(
                        base_ccy=base,
                        pair_kind="spot_swap",
                        spot_inst_id=spot_inst.inst_id,
                        derivative_inst_id=swap_inst.inst_id,
                        spot_ticker=spot_ticker,
                        derivative_ticker=swap_ticker,
                        config=self._config,
                        days_to_expiry=None,
                    )
                    if row is not None:
                        opportunities.append(row)

            quarter_pair = futures_map.get(base)
            if quarter_pair is None:
                continue
            quarter_inst, next_quarter_inst = quarter_pair
            for pair_kind, future_inst in (
                ("spot_quarter", quarter_inst),
                ("spot_next_quarter", next_quarter_inst),
            ):
                if future_inst is None:
                    continue
                future_ticker = futures_tickers.get(future_inst.inst_id)
                if future_ticker is None:
                    continue
                expiry = _parse_futures_expiry(future_inst.inst_id)
                days = _days_to_expiry(expiry) if expiry is not None else None
                row = _build_opportunity(
                    base_ccy=base,
                    pair_kind=pair_kind,  # type: ignore[arg-type]
                    spot_inst_id=spot_inst.inst_id,
                    derivative_inst_id=future_inst.inst_id,
                    spot_ticker=spot_ticker,
                    derivative_ticker=future_ticker,
                    config=self._config,
                    days_to_expiry=days,
                )
                if row is not None:
                    opportunities.append(row)

        opportunities.sort(key=lambda item: item.sort_key(), reverse=True)
        return opportunities
