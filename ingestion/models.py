"""SQLAlchemy ORM models for the raw schema.

Tables are created via create_all_tables(engine), which also provisions the
TimescaleDB hypertable and the DESC index — both are idempotent.
"""
from __future__ import annotations

from sqlalchemy import BOOLEAN, INTEGER, NUMERIC, TIMESTAMP, VARCHAR, Column, text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class OHLCV(Base):
    """raw.ohlcv — one row per (symbol, timestamp) candle.

    Converted to a TimescaleDB hypertable on first creation.
    """

    __tablename__ = "ohlcv"
    __table_args__ = {"schema": "raw"}

    symbol = Column(VARCHAR(20), primary_key=True, nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), primary_key=True, nullable=False)
    open = Column(NUMERIC(20, 8), nullable=False)
    high = Column(NUMERIC(20, 8), nullable=False)
    low = Column(NUMERIC(20, 8), nullable=False)
    close = Column(NUMERIC(20, 8), nullable=False)
    volume = Column(NUMERIC(30, 8), nullable=False)
    quote_volume = Column(NUMERIC(30, 8))
    trades = Column(INTEGER)
    taker_buy_vol = Column(NUMERIC(30, 8))


class Symbol(Base):
    """raw.symbols — reference table of tradeable pairs."""

    __tablename__ = "symbols"
    __table_args__ = {"schema": "raw"}

    symbol = Column(VARCHAR(20), primary_key=True)
    base_asset = Column(VARCHAR(10))
    quote_asset = Column(VARCHAR(10))
    is_active = Column(BOOLEAN, default=True, server_default=text("true"))
    created_at = Column(TIMESTAMP(timezone=True), server_default=text("now()"))


def create_all_tables(engine) -> None:
    """Create all tables, hypertable, and index — safe to call repeatedly."""
    Base.metadata.create_all(engine)
    with engine.connect() as conn:
        conn.execute(
            text(
                "SELECT create_hypertable('raw.ohlcv', 'timestamp', if_not_exists => TRUE)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_ts_desc"
                " ON raw.ohlcv (symbol, timestamp DESC)"
            )
        )
        conn.commit()
