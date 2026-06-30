"""Pure functions for rounding quantities/prices to Binance exchange filters.

No exchange I/O here — see BinanceClientWrapper.get_symbol_info_cached() for
the fetch+cache side. Everything here is deterministic and unit-testable
with plain dicts, no mocks required.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass
class SymbolFilters:
    step_size:    Decimal   # LOT_SIZE
    min_qty:      Decimal   # LOT_SIZE
    tick_size:    Decimal   # PRICE_FILTER
    min_notional: Decimal   # MIN_NOTIONAL (or NOTIONAL on newer symbols)


class FilterViolation(Exception):
    """Raised when a rounded qty/price/notional fails exchange minimums."""


def parse_symbol_filters(symbol_info: dict) -> SymbolFilters:
    """Extract step/tick/min-notional from client.get_symbol_info() output.

    Handles both the legacy MIN_NOTIONAL and the newer NOTIONAL filter type.
    """
    filters = {f["filterType"]: f for f in symbol_info["filters"]}
    lot = filters["LOT_SIZE"]
    price = filters["PRICE_FILTER"]
    notional = filters.get("MIN_NOTIONAL") or filters["NOTIONAL"]
    min_notional_val = notional.get("minNotional", notional.get("notional"))
    return SymbolFilters(
        step_size=Decimal(lot["stepSize"]),
        min_qty=Decimal(lot["minQty"]),
        tick_size=Decimal(price["tickSize"]),
        min_notional=Decimal(min_notional_val),
    )


def round_step(value: float, step: Decimal) -> float:
    """Floor *value* down to the nearest multiple of *step* (Decimal-exact)."""
    d = Decimal(str(value))
    if step == 0:
        return float(d)
    floored = (d // step) * step
    return float(floored)


def round_qty_down(qty: float, filters: SymbolFilters) -> float:
    """Round qty down to stepSize. Returns 0.0 if the result < minQty."""
    rounded = round_step(qty, filters.step_size)
    return rounded if rounded >= float(filters.min_qty) else 0.0


def round_price_to_tick(price: float, filters: SymbolFilters) -> float:
    """Round price down to tickSize (conservative direction for limit orders)."""
    return round_step(price, filters.tick_size)


def validate_notional(qty: float, price: float, filters: SymbolFilters) -> None:
    """Raise FilterViolation if qty*price is below the exchange minimum."""
    if Decimal(str(qty)) * Decimal(str(price)) < filters.min_notional:
        raise FilterViolation(
            f"notional {qty * price:.8f} below minNotional {filters.min_notional}"
        )
