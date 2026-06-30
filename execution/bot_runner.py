"""Main bot loop: generate signals → validate → execute every N seconds.

Usage:
    python -m execution.bot_runner --dry-run --once        # single cycle, no orders
    python -m execution.bot_runner --dry-run               # loop every hour
    python -m execution.bot_runner --once                  # single cycle, live testnet
    python -m execution.bot_runner --interval 1800         # loop every 30 min
"""
from __future__ import annotations

import os
import time
from urllib.parse import quote_plus

import structlog
from dotenv import load_dotenv
from sqlalchemy import create_engine

from execution.risk_manager import RiskManager
from execution.trade_executor import TradeExecutor
from ingestion.binance_client import BinanceClientWrapper
from signals.aggregator import SignalAggregator

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Cycle logic
# ---------------------------------------------------------------------------

def run_once(
    symbols:    list[str],
    executor:   TradeExecutor,
    aggregator: SignalAggregator,
    dry_run:    bool = False,
) -> None:
    """Generate signals for every symbol and attempt execution."""
    for symbol in symbols:
        try:
            signal = aggregator.aggregate(symbol)
            executor.execute_signal(signal, dry_run=dry_run)
        except Exception:
            log.exception("cycle_error", symbol=symbol)


def run_loop(
    symbols:          list[str],
    executor:         TradeExecutor,
    aggregator:       SignalAggregator,
    interval_seconds: int  = 3600,
    dry_run:          bool = False,
) -> None:
    """Run indefinitely, once per *interval_seconds*. Ctrl-C to stop."""
    log.info("bot_starting", interval_s=interval_seconds,
             symbols=symbols, dry_run=dry_run)
    try:
        while True:
            run_once(symbols, executor, aggregator, dry_run=dry_run)
            log.info("cycle_complete_sleeping", seconds=interval_seconds)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        log.info("bot_stopped")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Hybrid trading bot — testnet only"
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Run a single cycle then exit",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log trades without placing real orders",
    )
    parser.add_argument(
        "--interval", type=int, default=3600,
        help="Seconds between cycles in loop mode (default: 3600)",
    )
    args = parser.parse_args()

    load_dotenv()

    engine = create_engine(
        "postgresql+psycopg2://{u}:{p}@{h}:{port}/{db}".format(
            u=quote_plus(os.getenv("POSTGRES_USER",     "trader")),
            p=quote_plus(os.getenv("POSTGRES_PASSWORD", "")),
            h=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            db=os.getenv("POSTGRES_DB",   "trading"),
        )
    )

    client     = BinanceClientWrapper()
    risk       = RiskManager(client, engine)
    aggregator = SignalAggregator(engine)
    executor   = TradeExecutor(client, risk, engine)
    symbols    = [
        s.strip()
        for s in os.getenv("TRADING_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")
        if s.strip()
    ]

    try:
        if args.once:
            run_once(symbols, executor, aggregator, dry_run=args.dry_run)
        else:
            run_loop(symbols, executor, aggregator,
                     interval_seconds=args.interval, dry_run=args.dry_run)
    finally:
        engine.dispose()
