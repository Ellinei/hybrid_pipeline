"""Rule-based signal generator from dbt feature table.

Rules are evaluated top-to-bottom; first match wins.
All thresholds come from standard technical analysis literature.
"""
from __future__ import annotations

from datetime import datetime, timezone

import structlog
from sqlalchemy import text

from signals.base import Signal, SignalDirection

log = structlog.get_logger()


class TechnicalSignalGenerator:
    """Generates BUY / SELL / HOLD signals from ``features.feat_technical``."""

    def __init__(self, db_engine) -> None:
        self.engine = db_engine

    # ------------------------------------------------------------------
    # DB access
    # ------------------------------------------------------------------

    def get_latest_features(self, symbol: str) -> dict | None:
        """Return the most recent feature row for *symbol*, or None."""
        query = text("""
            SELECT
                rsi_14_proxy, macd_line, bb_zscore,
                pct_change_1h, pct_change_24h, log_return_1h,
                range_14, volume
            FROM features.feat_technical
            WHERE symbol = :symbol
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"symbol": symbol}).fetchone()
        return dict(row._mapping) if row else None

    # ------------------------------------------------------------------
    # Signal logic
    # ------------------------------------------------------------------

    def generate_signal(self, symbol: str) -> Signal:
        """Apply rule set and return a Signal."""
        now = datetime.now(timezone.utc)
        features = self.get_latest_features(symbol)

        if not features:
            return Signal(
                direction=SignalDirection.HOLD, confidence=0.5,
                source="technical", symbol=symbol, timestamp=now,
                metadata={},
            )

        rsi    = features.get("rsi_14_proxy",  50.0) or 50.0
        macd   = features.get("macd_line",      0.0) or 0.0
        bz     = features.get("bb_zscore",      0.0) or 0.0
        pct_1h = features.get("pct_change_1h",  0.0) or 0.0
        pct_24 = features.get("pct_change_24h", 0.0) or 0.0

        meta = {
            "rsi": rsi, "macd": macd, "bb_zscore": bz,
            "pct_change_1h": pct_1h, "pct_change_24h": pct_24,
        }

        def sig(direction: SignalDirection, confidence: float) -> Signal:
            return Signal(
                direction=direction, confidence=confidence,
                source="technical", symbol=symbol, timestamp=now,
                metadata=meta,
            )

        # ── Strong BUY ──────────────────────────────────────────────
        if rsi < 30 and macd > 0:
            return sig(SignalDirection.BUY, 0.8)
        if bz < -2.0:
            return sig(SignalDirection.BUY, 0.75)

        # ── Strong SELL ─────────────────────────────────────────────
        if rsi > 70 and macd < 0:
            return sig(SignalDirection.SELL, 0.8)
        if bz > 2.0:
            return sig(SignalDirection.SELL, 0.75)

        # ── Moderate BUY ────────────────────────────────────────────
        if rsi < 40 and pct_1h > 0:
            return sig(SignalDirection.BUY, 0.55)
        if macd > 0 and pct_24 > 2.0:
            return sig(SignalDirection.BUY, 0.6)

        # ── Moderate SELL ───────────────────────────────────────────
        if rsi > 60 and pct_1h < 0:
            return sig(SignalDirection.SELL, 0.55)
        if macd < 0 and pct_24 < -2.0:
            return sig(SignalDirection.SELL, 0.6)

        # ── Default ─────────────────────────────────────────────────
        return sig(SignalDirection.HOLD, 0.5)
