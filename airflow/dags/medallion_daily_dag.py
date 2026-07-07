"""Daily medallion pipeline: ingest -> bronze -> silver -> gold -> deliver.

Runs the packaged CLI so the DAG stays a thin orchestration layer: the exact
same entrypoints run locally, on Airflow workers, or as Databricks jobs.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

ENV = os.environ.get("LAKEHOUSE_ENV", "dev")

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=2),
    "sla": timedelta(hours=3),
}

with DAG(
    dag_id="medallion_daily",
    description="Kaggle/Reddit ingestion through the medallion layers into Snowflake.",
    schedule="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["medallion", "lakehouse", ENV],
) as dag:

    def stage(name: str) -> BashOperator:
        return BashOperator(
            task_id=name.replace("-", "_"),
            bash_command=f"LAKEHOUSE_ENV={ENV} python -m lakehouse.cli {name}",
        )

    with TaskGroup(group_id="medallion") as medallion:
        stage("ingest") >> stage("bronze") >> stage("silver") >> stage("gold")

    deliver = stage("deliver")
    medallion >> deliver
