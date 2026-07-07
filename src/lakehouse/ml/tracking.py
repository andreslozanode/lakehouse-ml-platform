"""Centralized MLflow initialization: single place for URIs, experiment and tags.

Every trainer and the registry call `init_mlflow(cfg)` so tracking behavior is
environment-driven (local server in dev, Databricks-managed MLflow in qa/prod)
and every run carries the same standard tags for lineage and auditability.
"""

from __future__ import annotations

import os
import subprocess

import mlflow

from lakehouse.common.config import Config
from lakehouse.common.logging import get_logger

log = get_logger(__name__)


def _git_sha() -> str:
    # CI provides the SHA; local runs fall back to the working tree.
    for var in ("GITHUB_SHA", "GIT_COMMIT"):
        if os.environ.get(var):
            return os.environ[var][:12]
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "--short=12", "HEAD"], stderr=subprocess.DEVNULL
            )
            .decode()
            .strip()
        )
    except Exception:
        return "unknown"


def standard_tags(cfg: Config) -> dict[str, str]:
    return {
        "project": cfg.project,
        "environment": cfg.environment,
        "git_sha": _git_sha(),
        "dataset": cfg.sources.kaggle.dataset,
    }


def init_mlflow(cfg: Config) -> None:
    """Configure tracking/registry URIs and the experiment for this run."""
    mlf = cfg.ml.mlflow
    mlflow.set_tracking_uri(mlf.tracking_uri)
    if mlf.registry_uri:
        mlflow.set_registry_uri(mlf.registry_uri)
    mlflow.set_experiment(mlf.experiment)
    log.info(
        "MLflow ready: tracking=%s registry=%s experiment=%s",
        mlflow.get_tracking_uri(),
        mlflow.get_registry_uri(),
        mlf.experiment,
    )


def registered_model_name(cfg: Config, family: str) -> str:
    return cfg.ml.mlflow.registered_models[family]
