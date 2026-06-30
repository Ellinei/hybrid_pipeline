"""Pure fill/exit simulation — fees, slippage, and stop/take-profit checks.

No I/O. The engine feeds real historical bars and positions in; this module
only does arithmetic.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from backtest.state import OpenPosition


@dataclass
class OHLCVBar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


def apply_slippage(price: float, slippage_bps: float, side: str) -> float:
    """Worsen *price* by slippage_bps basis points in the direction that
    hurts the trader: higher for a BUY fill, lower for a SELL fill."""
    factor = slippage_bps / 10_000.0
    return price * (1 + factor) if side == "BUY" else price * (1 - factor)


def simulate_entry_fill(
    price: float, qty: float, fee_rate: float, slippage_bps: float
) -> tuple[float, float]:
    """Returns (fill_price, fee_paid) for a market BUY entry."""
    fill_price = apply_slippage(price, slippage_bps, side="BUY")
    fee = fill_price * qty * fee_rate
    return fill_price, fee


def simulate_exit_fill(
    price: float, qty: float, fee_rate: float, slippage_bps: float
) -> tuple[float, float]:
    """Returns (fill_price, fee_paid) for a market SELL exit."""
    fill_price = apply_slippage(price, slippage_bps, side="SELL")
    fee = fill_price * qty * fee_rate
    return fill_price, fee


def check_exit(bar: OHLCVBar, position: OpenPosition) -> tuple[str, float] | None:
    """Returns (reason, exit_price) if stop_loss or take_profit was hit
    within this bar's [low, high] range, else None.

    Tie-break: if BOTH fall within the same bar's range, assume stop_loss
    filled first (conservative — without tick data the within-bar order is
    unknowable; this avoids overstating performance). Documented
    limitation, not a bug.
    """
    stop_hit = bar.low <= position.stop_loss
    target_hit = bar.high >= position.take_profit

    if stop_hit:
        return "stop_loss", position.stop_loss
    if target_hit:
        return "take_profit", position.take_profit
    return None
