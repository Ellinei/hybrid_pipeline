"""Reddit sentiment signal using VADER.

Fetches recent posts from crypto subreddits, scores with VADER compound
sentiment, and weighs by Reddit post score (upvotes).

Degrades gracefully: if Reddit credentials are missing or the API is
unavailable, ``generate_signal`` returns HOLD with confidence 0.4.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import structlog
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from signals.base import Signal, SignalDirection

log = structlog.get_logger()


class SentimentSignalGenerator:
    SUBREDDITS = ["cryptocurrency", "bitcoin", "ethtrader", "solana"]

    SYMBOL_KEYWORDS: dict[str, list[str]] = {
        "BTCUSDT": ["bitcoin", "btc", "bitcoin price"],
        "ETHUSDT": ["ethereum", "eth", "ether"],
        "SOLUSDT": ["solana", "sol", "solana price"],
    }

    def __init__(self) -> None:
        self.vader  = SentimentIntensityAnalyzer()
        self.reddit = None
        self._init_reddit()

    def _init_reddit(self) -> None:
        client_id     = os.getenv("REDDIT_CLIENT_ID",     "")
        client_secret = os.getenv("REDDIT_CLIENT_SECRET", "")
        username      = os.getenv("REDDIT_USERNAME",      "")
        password      = os.getenv("REDDIT_PASSWORD",      "")
        user_agent    = os.getenv("REDDIT_USER_AGENT",    "hybrid-trading-pipeline/1.0")

        if not all([client_id, client_secret, username, password]):
            log.info("reddit_credentials_missing_graceful_fallback")
            return

        try:
            import praw  # lazy import — praw is optional at module level
            self.reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                username=username,
                password=password,
                user_agent=user_agent,
            )
        except Exception as exc:
            log.warning("reddit_init_failed", error=str(exc))

    # ------------------------------------------------------------------
    # Post fetching
    # ------------------------------------------------------------------

    def fetch_posts(self, symbol: str, limit: int = 50) -> list[dict[str, Any]]:
        """Fetch and filter Reddit posts for *symbol*.  Empty list if unavailable."""
        if self.reddit is None:
            return []

        keywords = self.SYMBOL_KEYWORDS.get(symbol, [symbol.lower().replace("USDT", "")])
        posts: list[dict] = []

        for sub_name in self.SUBREDDITS:
            try:
                subreddit = self.reddit.subreddit(sub_name)
                for submission in subreddit.hot(limit=limit):
                    body = (submission.title + " " + (submission.selftext or "")).lower()
                    if any(kw in body for kw in keywords):
                        posts.append({
                            "title":       submission.title,
                            "selftext":    submission.selftext or "",
                            "score":       max(submission.score, 1),  # floor at 1
                            "created_utc": submission.created_utc,
                        })
            except Exception as exc:
                log.warning("reddit_fetch_failed", subreddit=sub_name, error=str(exc))

        return posts

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def score_posts(self, posts: list[dict[str, Any]]) -> float:
        """Return VADER-weighted compound score in [-1, 1].  0.0 if empty."""
        if not posts:
            return 0.0

        total_upvotes = sum(p["score"] for p in posts)
        if total_upvotes == 0:
            return 0.0

        weighted_sum = sum(
            self.vader.polarity_scores(p["title"] + " " + p["selftext"])["compound"]
            * p["score"]
            for p in posts
        )
        return weighted_sum / total_upvotes

    # ------------------------------------------------------------------
    # Signal
    # ------------------------------------------------------------------

    def generate_signal(self, symbol: str) -> Signal:
        now      = datetime.now(timezone.utc)
        posts    = self.fetch_posts(symbol)
        sentiment = self.score_posts(posts)

        if sentiment > 0.2:
            direction  = SignalDirection.BUY
            confidence = min(0.4 + sentiment * 0.3, 0.7)
        elif sentiment < -0.2:
            direction  = SignalDirection.SELL
            confidence = min(0.4 + abs(sentiment) * 0.3, 0.7)
        else:
            direction  = SignalDirection.HOLD
            confidence = 0.4

        return Signal(
            direction=direction, confidence=confidence,
            source="sentiment", symbol=symbol, timestamp=now,
            metadata={
                "sentiment_score":    sentiment,
                "post_count":         len(posts),
                "subreddits_checked": self.SUBREDDITS,
            },
        )
