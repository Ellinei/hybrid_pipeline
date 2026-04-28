"""XGBoost next-hour direction predictor.

Trains on ``features.feat_technical`` and predicts whether the next-hour
close will be higher than the current close (binary classification).

Usage:
    python -m signals.ml_model   # train and print metrics
"""
from __future__ import annotations

import os
import pickle
import sys
from datetime import datetime, timezone

import pandas as pd
import structlog
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text
from xgboost import XGBClassifier

from signals.base import Signal, SignalDirection

log = structlog.get_logger()


class MLSignalGenerator:
    """XGBoost classifier: predicts next-hour price direction."""

    MODEL_PATH  = "models/xgboost_signal.pkl"
    SCALER_PATH = "models/scaler.pkl"

    FEATURES = [
        "rsi_14_proxy", "macd_line", "bb_zscore",
        "pct_change_1h", "pct_change_24h", "log_return_1h",
        "range_14", "volume",
    ]

    def __init__(self, db_engine) -> None:
        self.engine = db_engine
        self.model:  XGBClassifier   | None = None
        self.scaler: StandardScaler  | None = None

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def load_training_data(self, symbol: str, min_rows: int = 200) -> pd.DataFrame:
        """Load features + next-hour target from the feature table."""
        query = text("""
            SELECT
                rsi_14_proxy, macd_line, bb_zscore,
                pct_change_1h, pct_change_24h, log_return_1h,
                range_14, volume,
                close,
                LEAD(close, 1) OVER (PARTITION BY symbol ORDER BY timestamp) AS next_close
            FROM features.feat_technical
            WHERE symbol = :symbol
            ORDER BY timestamp
        """)
        with self.engine.connect() as conn:
            result = conn.execute(query, {"symbol": symbol})
            rows   = result.fetchall()
            df     = pd.DataFrame(rows, columns=list(result.keys()))

        df = df.dropna(subset=["next_close"])
        df["target"] = (df["next_close"] > df["close"]).astype(int)
        df = df.dropna(subset=self.FEATURES + ["target"])

        if len(df) < min_rows:
            log.warning("insufficient_training_data", symbol=symbol, rows=len(df))
            return pd.DataFrame()

        return df[self.FEATURES + ["target"]]

    def train(self, symbols: list[str]) -> dict:
        """Train on all symbols combined.  Returns metrics dict, empty if no data."""
        frames = [self.load_training_data(sym) for sym in symbols]
        frames = [f for f in frames if not f.empty]

        if not frames:
            log.warning("no_training_data_for_any_symbol")
            return {}

        full = pd.concat(frames, ignore_index=True)
        X = full[self.FEATURES].values
        y = full["target"].values

        # Time-based split — no shuffle (preserve temporal order)
        split = int(len(X) * 0.8)
        X_tr, X_te = X[:split], X[split:]
        y_tr, y_te = y[:split], y[split:]

        self.scaler = StandardScaler()
        X_tr_s = self.scaler.fit_transform(X_tr)
        X_te_s  = self.scaler.transform(X_te)

        self.model = XGBClassifier(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            eval_metric="logloss",
            random_state=42,
        )
        self.model.fit(X_tr_s, y_tr)

        y_pred = self.model.predict(X_te_s)
        metrics = {
            "accuracy":  float(accuracy_score(y_te, y_pred)),
            "precision": float(precision_score(y_te, y_pred, zero_division=0)),
            "recall":    float(recall_score(y_te, y_pred, zero_division=0)),
            "f1":        float(f1_score(y_te, y_pred, zero_division=0)),
            "train_size": int(len(X_tr)),
            "test_size":  int(len(X_te)),
            "feature_importance": dict(
                zip(self.FEATURES, self.model.feature_importances_.tolist())
            ),
        }

        os.makedirs(os.path.dirname(self.MODEL_PATH), exist_ok=True)
        with open(self.MODEL_PATH,  "wb") as f: pickle.dump(self.model,  f)
        with open(self.SCALER_PATH, "wb") as f: pickle.dump(self.scaler, f)

        log.info("model_trained", **{k: v for k, v in metrics.items()
                                      if k != "feature_importance"})
        return metrics

    # ------------------------------------------------------------------
    # Inference
    # ------------------------------------------------------------------

    def is_trained(self) -> bool:
        return (os.path.exists(self.MODEL_PATH)
                and os.path.exists(self.SCALER_PATH))

    def load_model(self) -> bool:
        """Load persisted model + scaler.  Returns True on success."""
        if not self.is_trained():
            return False
        try:
            with open(self.MODEL_PATH,  "rb") as f: self.model  = pickle.load(f)
            with open(self.SCALER_PATH, "rb") as f: self.scaler = pickle.load(f)
            return True
        except Exception as exc:
            log.warning("model_load_failed", error=str(exc))
            return False

    def generate_signal(self, symbol: str) -> Signal:
        now = datetime.now(timezone.utc)

        if not self.load_model():
            return Signal(
                direction=SignalDirection.HOLD, confidence=0.5,
                source="ml", symbol=symbol, timestamp=now,
                metadata={"reason": "model_not_trained"},
            )

        query = text("""
            SELECT rsi_14_proxy, macd_line, bb_zscore,
                   pct_change_1h, pct_change_24h, log_return_1h,
                   range_14, volume
            FROM features.feat_technical
            WHERE symbol = :symbol
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"symbol": symbol}).fetchone()

        if row is None:
            return Signal(
                direction=SignalDirection.HOLD, confidence=0.5,
                source="ml", symbol=symbol, timestamp=now,
                metadata={"reason": "no_features_available"},
            )

        feat_values = [float(row[f]) for f in self.FEATURES]
        X = self.scaler.transform([feat_values])
        prob_up = float(self.model.predict_proba(X)[0][1])

        if prob_up > 0.65:
            direction, confidence = SignalDirection.BUY,  prob_up
        elif prob_up < 0.35:
            direction, confidence = SignalDirection.SELL, 1.0 - prob_up
        else:
            direction, confidence = SignalDirection.HOLD, 0.5

        return Signal(
            direction=direction, confidence=confidence,
            source="ml", symbol=symbol, timestamp=now,
            metadata={
                "probability_up": prob_up,
                "features": dict(zip(self.FEATURES, feat_values)),
            },
        )


# ---------------------------------------------------------------------------
# Entry point — train and report
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os
    from urllib.parse import quote_plus

    from dotenv import load_dotenv
    from sqlalchemy import create_engine

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

    syms = [s.strip() for s in os.getenv("TRADING_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")]
    gen  = MLSignalGenerator(engine)
    metrics = gen.train(syms)

    if not metrics:
        print("No training data. Run: rest_backfill → dbt run → signals.ml_model")
        sys.exit(1)

    print("\n=== Model Training Results ===")
    for k, v in metrics.items():
        if k == "feature_importance":
            continue
        print(f"  {k:15}: {v:.4f}" if isinstance(v, float) else f"  {k:15}: {v}")

    print("\n  Feature Importance:")
    for feat, imp in sorted(metrics["feature_importance"].items(), key=lambda x: -x[1]):
        print(f"    {feat:25}: {imp:.4f}")

    engine.dispose()
