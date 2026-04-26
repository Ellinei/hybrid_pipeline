"""Thin wrapper around the python-binance Client.

Provides:
- Exponential-backoff retry decorator
- fetch_ohlcv() → UTC-aware DataFrame
- get_current_price() / ping() helpers
- Symbols loaded from TRADING_SYMBOLS env var
"""
from __future__ import annotations

import functools
import os
import time
from datetime import datetime

import pandas as pd
import structlog
from binance.client import Client
from dotenv import load_dotenv

log = structlog.get_logger()

# Column names returned by Binance get_historical_klines
_KLINE_COLS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades", "taker_buy_vol",
    "taker_buy_quote_vol", "ignore",
]

_OHLCV_COLS = [
    "symbol", "timestamp", "open", "high", "low", "close",
    "volume", "quote_volume", "trades", "taker_buy_vol",
]


def _retry(max_retries: int = 3, backoff_base: float = 2.0):
    """Decorator: retry on any exception with exponential back-off."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    if attempt == max_retries - 1:
                        raise
                    wait = backoff_base ** attempt
                    log.warning(
                        "api_retry",
                        func=func.__name__,
                        attempt=attempt + 1,
                        wait_s=wait,
                        error=str(exc),
                    )
                    time.sleep(wait)

        return wrapper

    return decorator


class BinanceClientWrapper:
    """Binance REST API wrapper with testnet support and structured logging."""

    def __init__(self) -> None:
        load_dotenv()
        api_key = os.getenv("BINANCE_API_KEY", "")
        api_secret = os.getenv("BINANCE_API_SECRET", "")
        testnet = os.getenv("BINANCE_TESTNET", "true").lower() == "true"
        symbols_raw = os.getenv("TRADING_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")

        self.symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]
        self.client = Client(api_key, api_secret, testnet=testnet)
        self._log = structlog.get_logger().bind(testnet=testnet)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_symbols(self) -> list[str]:
        return self.symbols

    @_retry(max_retries=3)
    def fetch_ohlcv(
        self,
        symbol: str,
        interval: str = "1h",
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int = 1000,
    ) -> pd.DataFrame:
        """Fetch OHLCV candles and return a UTC-aware DataFrame.

        Returns an empty DataFrame (with correct columns) when the API
        returns no data.
        """
        start_ms = (
            str(int(start_date.timestamp() * 1000)) if start_date else None
        )
        end_ms = (
            str(int(end_date.timestamp() * 1000)) if end_date else None
        )

        klines = self.client.get_historical_klines(
            symbol, interval, start_ms, end_ms, limit=limit
        )

        if not klines:
            return pd.DataFrame(columns=_OHLCV_COLS)

        df = pd.DataFrame(klines, columns=_KLINE_COLS)
        df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["symbol"] = symbol

        for col in ("open", "high", "low", "close", "volume",
                    "quote_volume", "taker_buy_vol"):
            df[col] = df[col].astype(float)
        df["trades"] = df["trades"].astype(int)

        return df[_OHLCV_COLS]

    @_retry(max_retries=3)
    def get_current_price(self, symbol: str) -> float:
        """Return the latest mark price for a symbol."""
        ticker = self.client.get_symbol_ticker(symbol=symbol)
        return float(ticker["price"])

    def ping(self) -> bool:
        """Return True if the Binance server is reachable, False otherwise."""
        try:
            self.client.ping()
            return True
        except Exception as exc:
            self._log.warning("ping_failed", error=str(exc))
            return False
