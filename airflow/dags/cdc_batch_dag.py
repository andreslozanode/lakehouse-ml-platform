"""Hourly batch CDC sync (high-watermark JDBC extraction + Delta MERGE).

Complements the always-on streaming job: environments without Kafka, or
tables where minute-level latency is unnecessary, are covered here.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

ENV = os.environ.get("LAKEHOUSE_ENV", "dev")

with DAG(
    dag_id="cdc_batch_sync",
    description="Incremental watermark sync from Postgres into silver CDC tables.",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "sla": timedelta(minutes=45),
    },
    tags=["cdc", "batch", ENV],
) as dag:

    sync = BashOperator(
        task_id="cdc_batch",
        bash_command=f"LAKEHOUSE_ENV={ENV} python -m lakehouse.cli cdc-batch",
    )

    refresh_gold = BashOperator(
        task_id="refresh_gold_orders",
        bash_command=f"LAKEHOUSE_ENV={ENV} python -m lakehouse.cli gold",
    )

    sync >> refresh_gold
