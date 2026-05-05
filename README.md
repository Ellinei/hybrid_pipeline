# Hybrid Trading Pipeline

**Production-grade hybrid streaming + batch data pipeline for AI-driven crypto trading on Binance.**

![Build](https://img.shields.io/badge/build-passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue)
![Python](https://img.shields.io/badge/python-3.11-blue)

## Architecture Overview

See [docs/architecture.md](docs/architecture.md) for the full system diagram and design decisions.

The pipeline runs two layers in concert:

- **Streaming layer** — Binance WebSocket → Redpanda/Kafka → real-time feature processor writes VWAP, volatility, and trade-count snapshots to TimescaleDB every 60 seconds.
- **Batch layer** — Airflow orchestrates daily OHLCV backfills and dbt model runs that build clean staging views and a `features.feat_technical` table (RSI, MACD, Bollinger Bands, ATR) from the raw data.

Both layers feed a **multi-signal fusion engine** that combines rule-based technical analysis, an **XGBoost** direction model, and **VADER**-scored RSS sentiment into a single BUY / HOLD / SELL decision with confidence score.

## Tech Stack

| Layer | Technology |
|---|---|
| Market data | Binance API (WebSocket + REST, testnet-ready) |
| Message broker | **Redpanda** (Kafka-compatible, dual listeners) |
| Orchestration | **Apache Airflow 2.8** (custom Docker image) |
| Transformations | **dbt 1.7** — staging → intermediate → features |
| Storage | **PostgreSQL 15 + TimescaleDB** (hypertables) |
| ML model | **XGBoost** (next-hour price-direction classifier) |
| Sentiment | **VADER + public RSS** (CoinDesk, Cointelegraph, CryptoPanic) |
| Monitoring | **Streamlit** (Phase 6) |
| Containerisation | Docker Compose |

## Quick Start

```bash
git clone https://github.com/your-username/hybrid-trading-pipeline.git
cd hybrid-trading-pipeline

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

# Train the XGBoost direction model
poetry run python -m signals.ml_model

# Run the full signal report
poetry run python -m signals.aggregator
```

## Running the Streaming Pipeline

```bash
# Terminal 1: WebSocket → Kafka
poetry run python -m ingestion.ws_producer

# Terminal 2: Kafka → real-time features → Postgres
poetry run python -m streaming.feature_processor
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
│   │   ├── intermediate/       # int_returns (view)
│   │   └── features/           # feat_technical (table)
│   ├── tests/
│   └── README.md
├── signals/                    # Multi-signal fusion engine
│   ├── base.py                 # Signal + AggregatedSignal dataclasses
│   ├── technical.py            # Rule-based: RSI, MACD, Bollinger Bands
│   ├── ml_model.py             # XGBoost: next-hour direction
│   ├── sentiment.py            # VADER + RSS feeds
│   └── aggregator.py          # Weighted fusion → BUY / HOLD / SELL
├── models/                     # Saved ML artifacts (XGBoost + scaler .pkl)
├── execution/                  # (Phase 6) risk manager + order executor
├── dashboard/                  # (Phase 7) Streamlit monitoring app
├── tests/
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
- [ ] **Phase 6** — Risk manager, position sizing, live order execution on Binance
- [ ] **Phase 7** — Streamlit monitoring dashboard, P&L tracking, alerting

## License

MIT — see [LICENSE](LICENSE).
