"""Champion/challenger promotion in the MLflow Model Registry.

A challenger version is promoted to the 'champion' alias only if its logged
test F1-macro beats the current champion's by a configurable margin.
"""

from __future__ import annotations

import argparse

import mlflow
from mlflow.tracking import MlflowClient

from lakehouse.common.config import load_config
from lakehouse.common.logging import get_logger
from lakehouse.ml.tracking import init_mlflow

log = get_logger(__name__)
METRIC = "test_f1_macro"


def _metric_for(client: MlflowClient, name: str, version: str) -> float:
    run_id = client.get_model_version(name, version).run_id
    return mlflow.get_run(run_id).data.metrics.get(METRIC, -1.0)


def promote(model_name: str, min_uplift: float = 0.0) -> None:
    init_mlflow(load_config())
    client = MlflowClient()
    versions = client.search_model_versions(f"name='{model_name}'")
    if not versions:
        raise RuntimeError(f"No versions found for '{model_name}'.")

    challenger = max(versions, key=lambda v: int(v.version))
    challenger_score = _metric_for(client, model_name, challenger.version)

    try:
        champion = client.get_model_version_by_alias(model_name, "champion")
        champion_score = _metric_for(client, model_name, champion.version)
    except Exception:
        champion, champion_score = None, -1.0

    log.info(
        "challenger v%s %s=%.4f vs champion %s=%.4f",
        challenger.version,
        METRIC,
        challenger_score,
        METRIC,
        champion_score,
    )
    if challenger_score >= champion_score + min_uplift:
        client.set_registered_model_alias(model_name, "champion", challenger.version)
        log.info("Promoted v%s to 'champion'.", challenger.version)
    else:
        log.info("Challenger did not beat champion; no promotion.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="lakehouse-sentiment-classical")
    parser.add_argument("--min-uplift", type=float, default=0.0)
    args = parser.parse_args()
    promote(args.model, args.min_uplift)
