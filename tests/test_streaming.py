"""Unit tests for the streaming pipeline.

No real Kafka or Binance connections — all I/O is mocked.

Run:
    poetry run pytest tests/test_streaming.py -v
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from statistics import stdev

import pytest


# ---------------------------------------------------------------------------
# Environment fixture — runs before every test
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _test_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("TRADING_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")
    monkeypatch.setenv("BINANCE_API_KEY", "test_key")
    monkeypatch.setenv("BINANCE_API_SECRET", "test_secret")
    monkeypatch.setenv("BINANCE_TESTNET", "true")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_USER", "test_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test_pass")
    monkeypatch.setenv("POSTGRES_DB", "test_db")


# ---------------------------------------------------------------------------
# Canonical raw Binance WebSocket payloads (the inner "data" dict from
# a combined stream envelope like {"stream": "btcusdt@trade", "data": {...}})
# ---------------------------------------------------------------------------

_RAW_TRADE = {
    "e": "trade",
    "E": 1_609_459_200_000,   # event time ms  (2021-01-01 00:00:00 UTC)
    "s": "BTCUSDT",
    "t": 123456,              # trade id
    "p": "29000.50000000",    # price string
    "q": "0.00100000",        # quantity string
    "b": 88,
    "a": 50,
    "T": 1_609_459_200_000,   # trade time ms
    "m": False,               # is buyer the market maker?
    "M": True,
}

_RAW_MINI_TICKER = {
    "e": "24hrMiniTicker",
    "E": 1_609_459_200_000,
    "s": "ETHUSDT",
    "c": "730.50",            # close price
    "o": "720.00",            # open price
    "h": "740.00",            # high price
    "l": "715.00",            # low price
    "v": "45000.000",         # base volume
    "q": "32000000.00",       # quote volume
}


# ===========================================================================
# 1. Trade message serialisation
# ===========================================================================

class TestMessageSerialization:
    """test_message_serialization — trade event → correct JSON schema."""

    def test_trade_has_exactly_the_required_fields(self):
        from ingestion.ws_producer import _format_trade

        msg = _format_trade(_RAW_TRADE)
        assert set(msg.keys()) == {
            "stream_type", "symbol", "price", "quantity",
            "timestamp", "trade_id", "is_buyer_maker", "received_at",
        }

    def test_trade_stream_type_is_trade(self):
        from ingestion.ws_producer import _format_trade

        assert _format_trade(_RAW_TRADE)["stream_type"] == "trade"

    def test_trade_symbol_is_uppercase(self):
        from ingestion.ws_producer import _format_trade

        assert _format_trade(_RAW_TRADE)["symbol"] == "BTCUSDT"

    def test_trade_price_converted_to_float(self):
        from ingestion.ws_producer import _format_trade

        msg = _format_trade(_RAW_TRADE)
        assert isinstance(msg["price"], float)
        assert msg["price"] == pytest.approx(29000.5)

    def test_trade_quantity_converted_to_float(self):
        from ingestion.ws_producer import _format_trade

        msg = _format_trade(_RAW_TRADE)
        assert isinstance(msg["quantity"], float)
        assert msg["quantity"] == pytest.approx(0.001)

    def test_trade_id_preserved(self):
        from ingestion.ws_producer import _format_trade

        assert _format_trade(_RAW_TRADE)["trade_id"] == 123456

    def test_trade_is_buyer_maker_preserved(self):
        from ingestion.ws_producer import _format_trade

        assert _format_trade(_RAW_TRADE)["is_buyer_maker"] is False

    def test_trade_is_json_serialisable(self):
        from ingestion.ws_producer import _format_trade

        msg = _format_trade(_RAW_TRADE)
        reparsed = json.loads(json.dumps(msg))
        assert reparsed["trade_id"] == 123456
        assert reparsed["stream_type"] == "trade"


# ===========================================================================
# 2. miniTicker message serialisation
# ===========================================================================

class TestMiniTickerSerialization:
    """test_miniTicker_serialization — miniTicker event → correct JSON schema."""

    def test_miniticker_has_exactly_the_required_fields(self):
        from ingestion.ws_producer import _format_mini_ticker

        msg = _format_mini_ticker(_RAW_MINI_TICKER)
        assert set(msg.keys()) == {
            "stream_type", "symbol",
            "close_price", "open_price", "high_price", "low_price",
            "base_volume", "quote_volume",
            "timestamp", "received_at",
        }

    def test_miniticker_stream_type(self):
        from ingestion.ws_producer import _format_mini_ticker

        assert _format_mini_ticker(_RAW_MINI_TICKER)["stream_type"] == "miniTicker"

    def test_miniticker_symbol(self):
        from ingestion.ws_producer import _format_mini_ticker

        assert _format_mini_ticker(_RAW_MINI_TICKER)["symbol"] == "ETHUSDT"

    def test_miniticker_numeric_fields_are_float(self):
        from ingestion.ws_producer import _format_mini_ticker

        msg = _format_mini_ticker(_RAW_MINI_TICKER)
        for field in ("close_price", "open_price", "high_price", "low_price",
                      "base_volume", "quote_volume"):
            assert isinstance(msg[field], float), f"'{field}' must be float"

    def test_miniticker_close_price_value(self):
        from ingestion.ws_producer import _format_mini_ticker

        assert _format_mini_ticker(_RAW_MINI_TICKER)["close_price"] == pytest.approx(730.5)

    def test_miniticker_is_json_serialisable(self):
        from ingestion.ws_producer import _format_mini_ticker

        msg = _format_mini_ticker(_RAW_MINI_TICKER)
        reparsed = json.loads(json.dumps(msg))
        assert reparsed["symbol"] == "ETHUSDT"
        assert reparsed["stream_type"] == "miniTicker"


# ===========================================================================
# 3. Feature computation (pure functions — no DB, no Kafka)
# ===========================================================================

class TestFeatureComputation:
    """test_feature_computation — VWAP and volatility from mock tick data."""

    def test_vwap_weighted_by_volume(self):
        from streaming.feature_processor import TickRecord, compute_vwap

        now = datetime.now(timezone.utc)
        ticks = [
            TickRecord(now, price=100.0, quantity=1.0),
            TickRecord(now, price=200.0, quantity=3.0),
        ]
        # (100×1 + 200×3) / (1+3) = 700 / 4 = 175.0
        assert compute_vwap(ticks) == pytest.approx(175.0)

    def test_vwap_returns_none_for_empty_list(self):
        from streaming.feature_processor import compute_vwap

        assert compute_vwap([]) is None

    def test_vwap_returns_none_when_total_volume_is_zero(self):
        from streaming.feature_processor import TickRecord, compute_vwap

        now = datetime.now(timezone.utc)
        assert compute_vwap([TickRecord(now, price=100.0, quantity=0.0)]) is None

    def test_compute_features_trade_count_within_1m(self):
        from streaming.feature_processor import TickRecord, compute_features

        now = datetime.now(timezone.utc)
        # Buffer ordered oldest-first, as it would be in real usage
        ticks = [
            TickRecord(now - timedelta(seconds=90), price=98.0,  quantity=1.0),  # outside 1m
            TickRecord(now - timedelta(seconds=30), price=100.0, quantity=1.0),
            TickRecord(now - timedelta(seconds=20), price=101.0, quantity=1.0),
            TickRecord(now - timedelta(seconds=10), price=102.0, quantity=1.0),
        ]
        result = compute_features("BTCUSDT", ticks, now=now)
        assert result["trade_count_1m"] == 3

    def test_compute_features_last_price_is_most_recent(self):
        from streaming.feature_processor import TickRecord, compute_features

        now = datetime.now(timezone.utc)
        ticks = [
            TickRecord(now - timedelta(seconds=20), price=100.0, quantity=1.0),
            TickRecord(now - timedelta(seconds=10), price=105.0, quantity=1.0),  # newest
        ]
        result = compute_features("BTCUSDT", ticks, now=now)
        assert result["last_price"] == pytest.approx(105.0)

    def test_compute_features_volatility_matches_stdev(self):
        from streaming.feature_processor import TickRecord, compute_features

        now = datetime.now(timezone.utc)
        prices = [100.0, 101.0, 99.0, 102.0, 98.0]
        # oldest at index 0, newest at index -1, all within 1 minute
        ticks = [
            TickRecord(now - timedelta(seconds=(4 - i) * 5), price=p, quantity=1.0)
            for i, p in enumerate(prices)
        ]
        result = compute_features("BTCUSDT", ticks, now=now)
        assert result["volatility_1m"] == pytest.approx(stdev(prices))

    def test_compute_features_volatility_none_with_single_tick(self):
        """stdev requires ≥ 2 points — single-tick window yields None."""
        from streaming.feature_processor import TickRecord, compute_features

        now = datetime.now(timezone.utc)
        ticks = [TickRecord(now, price=100.0, quantity=1.0)]
        result = compute_features("BTCUSDT", ticks, now=now)
        assert result["volatility_1m"] is None

    def test_compute_features_empty_buffer_returns_none_last_price(self):
        from streaming.feature_processor import compute_features

        now = datetime.now(timezone.utc)
        result = compute_features("BTCUSDT", [], now=now)
        assert result["last_price"] is None


# ===========================================================================
# 4. Buffer window filtering
# ===========================================================================

class TestBufferWindow:
    """test_buffer_window — filter_window correctly splits 1m and 5m windows."""

    def test_filter_1m_and_5m_windows(self):
        from streaming.feature_processor import TickRecord, filter_window

        now = datetime.now(timezone.utc)
        ticks = [
            TickRecord(now - timedelta(minutes=10), price=102.0, quantity=1.0),  # outside 5m
            TickRecord(now - timedelta(seconds=90), price=101.0, quantity=1.0),  # in 5m, out of 1m
            TickRecord(now - timedelta(seconds=30), price=100.0, quantity=1.0),  # in 1m
        ]
        assert len(filter_window(ticks, minutes=1, now=now)) == 1
        assert len(filter_window(ticks, minutes=5, now=now)) == 2

    def test_filter_includes_tick_at_exact_boundary(self):
        from streaming.feature_processor import TickRecord, filter_window

        now = datetime.now(timezone.utc)
        boundary = TickRecord(now - timedelta(minutes=1), price=100.0, quantity=1.0)
        # Exactly at the cutoff — must be INCLUDED (>= semantics)
        assert len(filter_window([boundary], minutes=1, now=now)) == 1

    def test_filter_empty_buffer_returns_empty_list(self):
        from streaming.feature_processor import filter_window

        assert filter_window([], minutes=1, now=datetime.now(timezone.utc)) == []

    def test_filter_uses_real_current_time_when_now_not_provided(self):
        from streaming.feature_processor import TickRecord, filter_window

        # Tick from 2 minutes ago must fall outside the 1m window
        old = datetime.now(timezone.utc) - timedelta(minutes=2)
        assert filter_window([TickRecord(old, 100.0, 1.0)], minutes=1) == []

    def test_filter_keeps_all_ticks_when_all_in_window(self):
        from streaming.feature_processor import TickRecord, filter_window

        now = datetime.now(timezone.utc)
        ticks = [
            TickRecord(now - timedelta(seconds=30), price=100.0, quantity=1.0),
            TickRecord(now - timedelta(seconds=20), price=101.0, quantity=1.0),
            TickRecord(now - timedelta(seconds=10), price=102.0, quantity=1.0),
        ]
        assert len(filter_window(ticks, minutes=1, now=now)) == 3
