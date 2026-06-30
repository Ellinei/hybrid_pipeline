# Hybrid Trading Pipeline

**Production-grade hybrid streaming + batch data pipeline for AI-driven crypto trading on Binance.**

![Build](https://img.shields.io/badge/build-passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue)
![Python](https://img.shields.io/badge/python-3.13-blue)
![Tests](https://img.shields.io/badge/tests-159%20passing-brightgreen)

## Architecture Overview

See [docs/architecture.md](docs/architecture.md) for the full system diagram and design decisions.

The pipeline runs two layers in concert:

- **Streaming layer** — Binance WebSocket → Redpanda/Kafka → real-time feature processor writes VWAP, volatility, and trade-count snapshots to TimescaleDB every 60 seconds.
- **Batch layer** — Airflow orchestrates daily OHLCV backfills and dbt model runs that build clean staging views and a `features.feat_technical` table (25 technical features including RSI, MACD, Bollinger Bands, ATR, stochastic, candle structure) from the raw data.

Both layers feed a **multi-signal fusion engine** that combines rule-based technical analysis, an **XGBoost** direction model (18 features, hyperparameter-tuned via GridSearchCV + TimeSeriesSplit), and **VADER**-scored RSS sentiment into a single BUY / HOLD / SELL decision with confidence score.

Live execution places real **OCO (One-Cancels-Other) orders** on Binance — entry + stop-loss + take-profit atomically — with exchange filter compliance (lot size, min notional, price precision). A historical **backtest harness** replays the live signal/risk/execution logic against OHLCV history, and a parameter sweep identifies optimal `RiskConfig` settings.

## Tech Stack

| Layer | Technology |
|---|---|
| Market data | Binance API (WebSocket + REST, testnet-ready) |
| Message broker | **Redpanda** (Kafka-compatible, dual listeners) |
| Orchestration | **Apache Airflow 2.8** (custom Docker image) |
| Transformations | **dbt 1.7** — staging → intermediate → features |
| Storage | **PostgreSQL 15 + TimescaleDB** (hypertables) |
| ML model | **XGBoost** (next-hour price-direction classifier, 18 features) |
| Sentiment | **VADER + public RSS** (CoinDesk, Cointelegraph, CryptoPanic) |
| Execution | **Binance OCO orders** — stop-loss + take-profit on entry |
| Backtesting | Custom engine + 192-combo parameter sweep |
| Monitoring | **Streamlit** dashboard |
| Containerisation | Docker Compose |

## Quick Start

```bash
git clone https://github.com/Ellinei/hybrid_pipeline.git
cd hybrid_pipeline

# 1. Configure environment
cp .env.example .env      # fill in your Binance testnet API keys

# 2. Start all services (Postgres, Redpanda, Airflow)
make up

# 3. Install Python dependencies
poetry install

# 4. Create Kafka topics
poetry run python -m streaming.setup_topics

# 5. Backfill 90 days of OHLCV data
poetry run python -m ingestion.rest_backfill

# 6. Validate connectivity + data
poetry run python -m ingestion.validate
```

## Service URLs

| Service | Host (external) | Docker network (internal) |
|---|---|---|
| Airflow UI | http://localhost:8080 | — |
| Redpanda Console | http://localhost:8085 | — |
| Redpanda (Kafka) | `localhost:19092` | `redpanda:9092` |
| PostgreSQL (TimescaleDB) | `localhost:5433` | `postgres:5432` |

> **Port notes:** Postgres is on `5433` to avoid colliding with a native Windows postgres install.
> Redpanda runs two listeners — host scripts use `localhost:19092`, Docker-internal services use
> `redpanda:9092`. See [docs/architecture.md](docs/architecture.md) for details.

## Running the Signal Engine

```bash
# Build dbt feature models (run after backfill)
cd dbt
DBT_POSTGRES_HOST=localhost DBT_POSTGRES_PORT=5433 \
DBT_POSTGRES_USER=trader DBT_POSTGRES_PASSWORD=test \
DBT_POSTGRES_DB=trading \
dbt deps --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir .
cd ..

# Train the XGBoost direction model (runs GridSearchCV hyperparameter tuning)
poetry run python -m signals.ml_model

# Run the full signal report
poetry run python -m signals.aggregator
```

## Running the Streaming Pipeline

```bash
# Terminal 1: WebSocket -> Kafka
poetry run python -m ingestion.ws_producer

# Terminal 2: Kafka -> real-time features -> Postgres
poetry run python -m streaming.feature_processor
```

## Backtesting

```bash
# Replay the full strategy against historical OHLCV data
python -m backtest.run_backtest

# Grid sweep RiskConfig parameters (192 combinations)
python -m backtest.sweep --top 15 --output sweep_results.csv

# Fine-grained sweep focused on stop-loss tuning (72 combinations)
python -m backtest.sweep --fine --top 15
```

## Project Structure

```
hybrid-trading-pipeline/
├── docker-compose.yml
├── pyproject.toml              # Poetry deps + ruff/pytest config
├── Makefile                    # Dev workflow shortcuts
├── .env.example
├── docs/
│   └── architecture.md
├── ingestion/                  # Binance REST backfill + WebSocket producer
│   ├── binance_client.py
│   ├── ws_producer.py
│   ├── rest_backfill.py
│   ├── models.py               # SQLAlchemy ORM + TimescaleDB hypertables
│   └── validate.py
├── streaming/                  # Kafka consumer + real-time feature processor
│   ├── feature_processor.py
│   └── setup_topics.py
├── airflow/
│   ├── Dockerfile              # Custom image: Airflow + dbt + project deps
│   ├── requirements.txt
│   └── dags/
│       ├── ohlcv_backfill_dag.py
│       └── dbt_features_dag.py
├── dbt/                        # Staging / intermediate / features models
│   ├── models/
│   │   ├── staging/            # stg_ohlcv (view)
│   │   ├── intermediate/       # int_returns (view) — log returns, pct changes
│   │   └── features/           # feat_technical (table) — 25 ML features
│   ├── tests/
│   └── README.md
├── signals/                    # Multi-signal fusion engine
│   ├── base.py                 # Signal + AggregatedSignal dataclasses
│   ├── technical.py            # Rule-based: RSI, MACD, Bollinger Bands
│   ├── ml_model.py             # XGBoost: next-hour direction, 18 features
│   ├── sentiment.py            # VADER + RSS feeds
│   └── aggregator.py           # Weighted fusion -> BUY / HOLD / SELL
├── execution/                  # Live order execution
│   ├── risk_manager.py         # RiskConfig, position sizing, ATR stops
│   ├── trade_executor.py       # Binance OCO order placement
│   ├── exchange_filters.py     # Lot size, min notional, price precision
│   └── bot_runner.py           # Main trading loop
├── backtest/                   # Historical strategy evaluation
│   ├── engine.py               # Replay engine — identical logic to live
│   ├── sim_risk_manager.py     # SimulatedRiskManager (overrides I/O only)
│   ├── sweep.py                # 192-combo + 72-combo fine grid sweep
│   ├── metrics.py              # Sharpe, profit factor, max drawdown
│   ├── exit_simulation.py      # Stop-loss / take-profit bar simulation
│   └── run_backtest.py         # Entry point
├── dashboard/
│   └── app.py                  # Streamlit P&L and signal monitoring
├── models/                     # Saved ML artifacts (XGBoost + scaler .pkl)
├── tests/                      # 159 tests covering all layers
└── docker/
    └── postgres/
        └── init.sql
```

## Roadmap

- [x] **Phase 1** — Project scaffold, Docker services, TimescaleDB schema init
- [x] **Phase 2** — Binance REST client, 90-day OHLCV backfill, validation script
- [x] **Phase 3** — Binance WebSocket producer, Kafka consumer, real-time features
- [x] **Phase 4** — Custom Airflow image, dbt staging → intermediate → features, 2 DAGs
- [x] **Phase 5** — Technical + XGBoost + RSS-sentiment signal engine, weighted fusion
- [x] **Phase 6** — Risk manager, position sizing, ATR-based stop/take-profit calculation
- [x] **Phase 7** — Live Binance OCO order execution, exchange filter compliance, bot runner, Streamlit dashboard
- [x] **Phase 8** — Historical backtest harness; SimulatedRiskManager runs identical approve/size/stop logic as live; chronological multi-symbol replay; Sharpe/PF/drawdown metrics
- [x] **Phase 8.5** — 192-combo RiskConfig parameter sweep with `_CachingAggregator` (pre-computes signals once, 192x in-memory replay)
- [x] **Phase 8.6** — Feature engineering: 18 ML features (up from 8); dbt models extended with candle structure, multi-horizon returns, trend position, volume conviction; accuracy 50.3% → 54.2%, win rate 32% → 69%
- [x] **Phase 8.7** — XGBoost hyperparameter tuning via GridSearchCV + TimeSeriesSplit; fine-grained 72-combo stop-loss sweep; first profitable backtest configs found (PF 1.19, +0.37% return at SL=1.0xATR, conf>=0.62, size=1.5%)
- [ ] **Phase 9** — Walk-forward ML retraining (rolling window; eliminates in-sample bias in backtest)
- [ ] **Phase 10** — Enable testnet live trading once walk-forward confirms positive expected value

## License

MIT — see [LICENSE](LICENSE).
