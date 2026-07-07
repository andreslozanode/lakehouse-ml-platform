"""Weekly ML retraining: features -> (classical || bert || tensorflow) -> promotion.

The three trainers run in parallel over identical splits; champion/challenger
promotion happens only after every trainer has reported its metrics.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

ENV = os.environ.get("LAKEHOUSE_ENV", "dev")

with DAG(
    dag_id="ml_weekly_training",
    description="Retrain sentiment models over the latest gold feature table.",
    schedule="0 3 * * 0",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "ml-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=6),
    },
    tags=["ml", "training", ENV],
) as dag:

    features = BashOperator(
        task_id="build_splits",
        bash_command=f"LAKEHOUSE_ENV={ENV} python -m lakehouse.ml.features",
    )

    trainers = [
        BashOperator(
            task_id=f"train_{name}",
            bash_command=f"LAKEHOUSE_ENV={ENV} python -m lakehouse.ml.train_{name}",
        )
        for name in ("classical", "bert_torch", "tensorflow")
    ]

    promote = BashOperator(
        task_id="promote_champion",
        bash_command=(
            f"LAKEHOUSE_ENV={ENV} python -m lakehouse.ml.registry "
            "--model lakehouse-sentiment-classical"
        ),
    )

    features >> trainers >> promote
