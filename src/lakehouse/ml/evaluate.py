"""Unified evaluation utilities shared by every trainer."""

from __future__ import annotations

import contextlib

import numpy as np
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

from lakehouse.common.logging import get_logger

log = get_logger(__name__)


def compute_metrics(
    y_true: np.ndarray, y_pred: np.ndarray, y_proba: np.ndarray | None = None
) -> dict[str, float]:
    metrics = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "f1_macro": float(f1_score(y_true, y_pred, average="macro")),
        "precision_macro": float(precision_score(y_true, y_pred, average="macro")),
        "recall_macro": float(recall_score(y_true, y_pred, average="macro")),
    }
    if y_proba is not None:
        with contextlib.suppress(ValueError):
            metrics["roc_auc"] = float(roc_auc_score(y_true, y_proba))
    return metrics


def log_report(name: str, y_true: np.ndarray, y_pred: np.ndarray) -> None:
    log.info("[%s]\n%s", name, classification_report(y_true, y_pred, digits=4))
