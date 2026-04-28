"""Daily incremental OHLCV backfill from Binance REST API.

Runs at 00:05 UTC, just before the dbt DAG at 00:15 UTC.
Operates on the last 2 days to catch any gaps without re-fetching full history.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _run_backfill() -> None:
    # Override host/port for the docker-internal network — the env vars in
    # docker-compose point to postgres:5432, but explicitly set them here so
    # this function works even if someone calls it outside Airflow.
    os.environ["POSTGRES_HOST"] = "postgres"
    os.environ["POSTGRES_PORT"] = "5432"

    from ingestion.rest_backfill import run_backfill

    symbols_raw = os.getenv("TRADING_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")
    symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]
    run_backfill(symbols=symbols, days=2, interval="1h")


with DAG(
    dag_id="ohlcv_backfill",
    default_args=default_args,
    description="Daily incremental OHLCV backfill from Binance",
    schedule="5 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ingestion", "binance"],
) as dag:

    backfill_task = PythonOperator(
        task_id="run_ohlcv_backfill",
        python_callable=_run_backfill,
    )
