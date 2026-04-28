# hybrid_trading dbt project

Transforms raw OHLCV bars from TimescaleDB into tested, analysis-ready feature tables.

## Model layers

| Layer | Schema | Materialisation | Purpose |
|---|---|---|---|
| `staging/` | `staging` | view | Clean + type-cast raw source; deduplicated |
| `intermediate/` | `staging` | view | Log returns and percentage changes |
| `features/` | `features` | table | Technical indicators for the signal engine |

Intermediate models share the `staging` schema to keep the feature schema clean.

## Running locally

Export the connection env vars, then run from the `dbt/` directory:

```bash
export DBT_POSTGRES_HOST=localhost
export DBT_POSTGRES_PORT=5433
export DBT_POSTGRES_USER=trader
export DBT_POSTGRES_PASSWORD=test
export DBT_POSTGRES_DB=trading

dbt deps --profiles-dir .           # install dbt_utils
dbt run  --profiles-dir .           # build all models
dbt test --profiles-dir .           # run data quality tests
```

Port `5433` is the host-side mapping for the Docker postgres container.

## Running inside Airflow

The `dbt_features` DAG runs `dbt deps → dbt run → dbt test` daily at 00:15 UTC.
Inside the Docker network the connection uses `postgres:5432` (internal hostname + port).
All credentials come from the `DBT_POSTGRES_*` environment variables set in `docker-compose.yml`.

## Models

### `stg_ohlcv`
Cleaned view of `raw.ohlcv`. Filters `close > 0` and `volume >= 0`.
Uniqueness tested on `(symbol, timestamp)`.

### `int_returns`
Log returns and percentage price changes over 1h, 24h, and 7d horizons using
`lag()` window functions partitioned by symbol.

### `feat_technical`
Physical table (refreshed on each dbt run) joining returns + OHLCV to compute:
- **RSI-14 proxy** — position of close within 14-period high/low range
- **MACD line** — 12-period MA minus 26-period MA
- **Bollinger Bands** — 20-period SMA ± 2σ, plus z-score
- **ATR proxy** — 14-period high/low range
