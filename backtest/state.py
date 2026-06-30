"""In-memory simulation state for the backtester — no DB, no exchange."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class OpenPosition:
    symbol: str
    quantity: float
    entry_price: float
    entry_time: datetime
    stop_loss: float
    take_profit: float
    entry_fee: float = 0.0


@dataclass
class BacktestState:
    balance: float
    positions: dict[str, OpenPosition] = field(default_factory=dict)
    closed_trades: list[dict] = field(default_factory=list)
    equity_curve: list[tuple[datetime, float]] = field(default_factory=list)
    current_atr: dict[str, float] = field(default_factory=dict)
