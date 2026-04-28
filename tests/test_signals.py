"""Unit tests for the multi-signal engine.

No real DB, Binance, or Reddit connections — all I/O is mocked.

Run:
    poetry run pytest tests/test_signals.py -v
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from signals.base import AggregatedSignal, Signal, SignalDirection


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _test_env(monkeypatch):
    """Predictable env for all signal tests — no real external connections."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_USER", "test_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test_pass")
    monkeypatch.setenv("POSTGRES_DB", "test_db")
    monkeypatch.setenv("TRADING_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")
    # Absent Reddit credentials → graceful degradation in SentimentSignalGenerator
    monkeypatch.delenv("REDDIT_CLIENT_ID",     raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("REDDIT_USERNAME",      raising=False)
    monkeypatch.delenv("REDDIT_PASSWORD",      raising=False)


def _features(**overrides) -> dict:
    """Build a minimal features dict with sensible defaults."""
    base = {
        "rsi_14_proxy":  50.0,
        "macd_line":      0.0,
        "bb_zscore":      0.0,
        "pct_change_1h":  0.0,
        "pct_change_24h": 0.0,
        "log_return_1h":  0.001,
        "range_14":    1000.0,
        "volume":       100.0,
    }
    base.update(overrides)
    return base


def _signal(direction: SignalDirection, confidence: float, source: str) -> Signal:
    return Signal(
        direction=direction,
        confidence=confidence,
        source=source,
        symbol="BTCUSDT",
        timestamp=datetime.now(timezone.utc),
        metadata={},
    )


# ===========================================================================
# 1 + 2 + 3.  Technical signal rules
# ===========================================================================

class TestTechnicalSignals:

    @patch("signals.technical.TechnicalSignalGenerator.get_latest_features")
    def test_technical_buy_signal(self, mock_feat):
        """RSI < 30 AND MACD > 0 → strong BUY, confidence 0.8."""
        from signals.technical import TechnicalSignalGenerator

        mock_feat.return_value = _features(rsi_14_proxy=25.0, macd_line=0.5)
        signal = TechnicalSignalGenerator(MagicMock()).generate_signal("BTCUSDT")

        assert signal.direction  == SignalDirection.BUY
        assert signal.confidence == pytest.approx(0.8)
        assert signal.source     == "technical"

    @patch("signals.technical.TechnicalSignalGenerator.get_latest_features")
    def test_technical_sell_signal(self, mock_feat):
        """RSI > 70 AND MACD < 0 → strong SELL, confidence 0.8."""
        from signals.technical import TechnicalSignalGenerator

        mock_feat.return_value = _features(rsi_14_proxy=75.0, macd_line=-0.5)
        signal = TechnicalSignalGenerator(MagicMock()).generate_signal("BTCUSDT")

        assert signal.direction  == SignalDirection.SELL
        assert signal.confidence == pytest.approx(0.8)

    @patch("signals.technical.TechnicalSignalGenerator.get_latest_features")
    def test_technical_hold_signal(self, mock_feat):
        """RSI=50, MACD=0 → no rule matches → HOLD."""
        from signals.technical import TechnicalSignalGenerator

        mock_feat.return_value = _features(rsi_14_proxy=50.0, macd_line=0.0)
        signal = TechnicalSignalGenerator(MagicMock()).generate_signal("BTCUSDT")

        assert signal.direction  == SignalDirection.HOLD
        assert signal.confidence == pytest.approx(0.5)

    @patch("signals.technical.TechnicalSignalGenerator.get_latest_features")
    def test_buy_signal_bb_below_lower_band(self, mock_feat):
        """BB zscore < -2.0 → BUY, confidence 0.75."""
        from signals.technical import TechnicalSignalGenerator

        mock_feat.return_value = _features(rsi_14_proxy=50.0, macd_line=0.0, bb_zscore=-2.5)
        signal = TechnicalSignalGenerator(MagicMock()).generate_signal("BTCUSDT")

        assert signal.direction  == SignalDirection.BUY
        assert signal.confidence == pytest.approx(0.75)

    @patch("signals.technical.TechnicalSignalGenerator.get_latest_features")
    def test_hold_when_no_features_in_db(self, mock_feat):
        """No rows in DB → HOLD fallback."""
        from signals.technical import TechnicalSignalGenerator

        mock_feat.return_value = None
        signal = TechnicalSignalGenerator(MagicMock()).generate_signal("BTCUSDT")

        assert signal.direction == SignalDirection.HOLD


# ===========================================================================
# 4.  ML signal — no model file on disk
# ===========================================================================

class TestMLSignal:

    @patch("signals.ml_model.MLSignalGenerator.is_trained", return_value=False)
    def test_ml_signal_no_model(self, _):
        """When model files don't exist → HOLD with confidence 0.5."""
        from signals.ml_model import MLSignalGenerator

        signal = MLSignalGenerator(MagicMock()).generate_signal("BTCUSDT")

        assert signal.direction  == SignalDirection.HOLD
        assert signal.confidence == pytest.approx(0.5)
        assert signal.source     == "ml"


# ===========================================================================
# 5.  Sentiment — graceful degradation without Reddit credentials
# ===========================================================================

class TestSentimentSignal:

    def test_sentiment_no_credentials(self):
        """Missing Reddit creds → reddit=None → HOLD, confidence 0.4."""
        from signals.sentiment import SentimentSignalGenerator

        gen = SentimentSignalGenerator()

        assert gen.reddit is None

        signal = gen.generate_signal("BTCUSDT")

        assert signal.direction  == SignalDirection.HOLD
        assert signal.confidence == pytest.approx(0.4)
        assert signal.source     == "sentiment"

    def test_empty_posts_returns_neutral_score(self):
        from signals.sentiment import SentimentSignalGenerator

        assert SentimentSignalGenerator().score_posts([]) == pytest.approx(0.0)

    def test_vader_scores_positive_text(self):
        from signals.sentiment import SentimentSignalGenerator

        posts = [{"title": "Bitcoin is incredible and amazing!", "selftext": "", "score": 100}]
        score = SentimentSignalGenerator().score_posts(posts)
        assert score > 0.2, f"Expected positive score, got {score}"


# ===========================================================================
# 6 + 7.  Aggregator — weights and exception fallback
# ===========================================================================

class TestSignalAggregator:

    def _agg(self, tech, ml, sent, tc=0.8, mc=0.7, sc=0.6):
        """Build an aggregator with mocked generators. Bypasses __init__."""
        from signals.aggregator import SignalAggregator

        agg = SignalAggregator.__new__(SignalAggregator)
        agg.weights = {"technical": 0.40, "ml": 0.40, "sentiment": 0.20}
        agg.technical = MagicMock()
        agg.ml        = MagicMock()
        agg.sentiment = MagicMock()
        agg.technical.generate_signal.return_value = _signal(tech, tc, "technical")
        agg.ml.generate_signal.return_value        = _signal(ml,   mc, "ml")
        agg.sentiment.generate_signal.return_value = _signal(sent, sc, "sentiment")
        return agg

    def test_aggregator_weights_all_buy_produces_buy(self):
        """test_aggregator_weights — three BUY signals → final BUY."""
        agg = self._agg(SignalDirection.BUY, SignalDirection.BUY, SignalDirection.BUY)
        result = agg.aggregate("BTCUSDT")
        assert result.direction == SignalDirection.BUY

    def test_aggregator_all_sell_produces_sell(self):
        agg = self._agg(SignalDirection.SELL, SignalDirection.SELL, SignalDirection.SELL)
        result = agg.aggregate("BTCUSDT")
        assert result.direction == SignalDirection.SELL

    def test_aggregator_symmetric_buy_sell_gives_hold(self):
        # BUY(0.5)*0.4 + SELL(0.5)*0.4 + HOLD(0.5)*0.2 = 0.0 → HOLD
        agg = self._agg(
            SignalDirection.BUY,  SignalDirection.SELL, SignalDirection.HOLD,
            tc=0.5, mc=0.5, sc=0.5,
        )
        result = agg.aggregate("BTCUSDT")
        assert result.direction == SignalDirection.HOLD

    def test_aggregator_fallback_one_generator_throws(self):
        """test_aggregator_fallback — one generator raises, others still used."""
        from signals.aggregator import SignalAggregator

        agg = SignalAggregator.__new__(SignalAggregator)
        agg.weights   = {"technical": 0.40, "ml": 0.40, "sentiment": 0.20}
        agg.technical = MagicMock()
        agg.ml        = MagicMock()
        agg.sentiment = MagicMock()

        agg.technical.generate_signal.side_effect = RuntimeError("DB gone")
        agg.ml.generate_signal.return_value        = _signal(SignalDirection.BUY, 0.8, "ml")
        agg.sentiment.generate_signal.return_value = _signal(SignalDirection.BUY, 0.6, "sentiment")

        result = agg.aggregate("BTCUSDT")

        assert result is not None
        assert isinstance(result, AggregatedSignal)
        # technical falls back to HOLD → ml+sentiment still push BUY
        # 0*0.4 + 0.8*0.4 + 0.6*0.2 = 0.32 + 0.12 = 0.44 > 0.15 → BUY
        assert result.direction == SignalDirection.BUY


# ===========================================================================
# 8.  Reasoning string contains all three source names
# ===========================================================================

class TestSignalReasoningString:

    def test_signal_reasoning_string(self):
        """test_signal_reasoning_string — all three sources appear in reasoning."""
        from signals.aggregator import SignalAggregator

        agg = SignalAggregator.__new__(SignalAggregator)
        agg.weights   = {"technical": 0.40, "ml": 0.40, "sentiment": 0.20}
        agg.technical = MagicMock()
        agg.ml        = MagicMock()
        agg.sentiment = MagicMock()
        agg.technical.generate_signal.return_value = _signal(SignalDirection.BUY,  0.8, "technical")
        agg.ml.generate_signal.return_value        = _signal(SignalDirection.HOLD, 0.5, "ml")
        agg.sentiment.generate_signal.return_value = _signal(SignalDirection.SELL, 0.4, "sentiment")

        result = agg.aggregate("BTCUSDT")

        reasoning_lower = result.reasoning.lower()
        assert "technical"  in reasoning_lower
        assert "ml"         in reasoning_lower
        assert "sentiment"  in reasoning_lower
