# Hybrid Trading Pipeline

**Production-grade hybrid streaming + batch data pipeline for AI-driven crypto trading on Binance.**

![Build](https://img.shields.io/badge/build-passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue)
![Python](https://img.shields.io/badge/python-3.11-blue)

## Architecture Overview

See [docs/architecture.md](docs/architecture.md) for the full system diagram and design decisions.

The pipeline operates on two layers working in concert: a **streaming layer** (Redpanda/Kafka) that captures tick-level trade and order book data from the Binance WebSocket API in real time, and a **batch layer** (Airflow + dbt) that orchestrates hourly feature engineering, model retraining, and signal validation. Both layers feed a **multi-signal fusion engine** that combines technical indicators, an **XGBoost** model, and (Phase 4) **FinBERT** sentiment scores before routing orders through a built-in risk manager.

## Tech Stack

| Layer | Technology |
|---|---|
| Market data | Binance API (WebSocket + REST) |
| Message broker | **Redpanda** (Kafka-compatible) |
| Orchestration | **Apache Airflow 2.8** |
| Transformations | **dbt** (testable SQL models) |
| Storage | **PostgreSQL 15 + TimescaleDB** |
| ML model | **XGBoost** |
| Sentiment (Phase 4) | FinBERT |
| Monitoring | **Streamlit** |
| Containerisation | Docker Compose |

## Quick Start

```bash
git clone https://github.com/your-username/hybrid-trading-pipeline.git
cd hybrid-trading-pipeline
cp .env.example .env   # fill in your Binance testnet keys
make up
```

## Service URLs

| Service | Host (external) | Docker network (internal) |
|---|---|---|
| Airflow UI | http://localhost:8080 | — |
| Redpanda Console | http://localhost:8085 | — |
| Redpanda (Kafka) | `localhost:19092` | `redpanda:9092` |
| PostgreSQL (TimescaleDB) | `localhost:5433` | `postgres:5432` |

> Postgres is mapped to `5433` to avoid colliding with a native Windows postgres install.
> Redpanda runs two listeners — host scripts must use `localhost:19092`, services on the
> docker network use `redpanda:9092`. See [docs/architecture.md](docs/architecture.md).

## Project Structure

```
hybrid-trading-pipeline/
├── docker-compose.yml
├── pyproject.toml          # Poetry deps + ruff/pytest config
├── Makefile                # Dev workflow shortcuts
├── .env.example
├── docs/
│   └── architecture.md
├── ingestion/              # Binance WebSocket + REST clients (Phase 2)
├── streaming/              # Kafka producers/consumers (Phase 2)
├── airflow/
│   └── dags/               # Airflow DAGs (Phase 3+)
├── dbt/                    # dbt models for feature engineering (Phase 3)
├── signals/                # Technical analysis + ML signal engine (Phase 3)
├── execution/              # Risk manager + order executor (Phase 5)
├── dashboard/              # Streamlit monitoring app (Phase 6)
├── tests/
├── notebooks/              # Exploratory analysis
└── docker/
    └── postgres/
        └── init.sql
```

## Roadmap

- [x] **Phase 1** — Project scaffold, Docker services, TimescaleDB schema init
- [x] **Phase 2** — Binance REST client, OHLCV historical backfill, validation script
- [x] **Phase 3** — Binance WebSocket producer, Kafka real-time feature processor
- [ ] **Phase 4** — dbt feature engineering, XGBoost signal model, Airflow DAGs
- [ ] **Phase 5** — Reddit sentiment ingestion, **FinBERT** NLP scoring
- [ ] **Phase 6** — **Multi-signal fusion**, risk management, live order execution
- [ ] **Phase 7** — Streamlit monitoring dashboard, alerting, performance reporting

## License

MIT — see [LICENSE](LICENSE).
