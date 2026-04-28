"""Daily dbt run: deps → run → test.

Scheduled at 00:15 UTC, 10 minutes after the OHLCV backfill DAG (00:05 UTC).
Uses BashOperator so dbt runs in its own process and inherits the container env vars.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

DBT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="dbt_features",
    default_args=default_args,
    description="Build and test dbt feature models (staging → intermediate → features)",
    schedule="15 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["transform", "dbt"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir {DBT_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR}",
    )

    dbt_deps >> dbt_run >> dbt_test
