"""Shared types for the multi-signal engine.

All signal generators produce ``Signal`` objects.  The aggregator produces an
``AggregatedSignal`` combining all three sources into a single trade decision.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class SignalDirection(Enum):
    BUY  = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass
class Signal:
    """A trade signal from one source."""

    direction:  SignalDirection
    confidence: float          # 0.0 – 1.0
    source:     str            # "technical" | "ml" | "sentiment"
    symbol:     str
    timestamp:  datetime
    metadata:   dict = field(default_factory=dict)


@dataclass
class AggregatedSignal:
    """Fused signal from all three sources."""

    direction:    SignalDirection
    confidence:   float
    symbol:       str
    timestamp:    datetime
    signals:      list[Signal]
    reasoning:    str
    weights_used: dict
