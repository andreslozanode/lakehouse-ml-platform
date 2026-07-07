"""Classical baselines: TF-IDF + KNN and TF-IDF + RandomForest, tracked in MLflow.

The two models share the exact same splits and preprocessing so the comparison
is apples-to-apples; the champion (by validation F1-macro) is registered.
"""

from __future__ import annotations

import mlflow
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import GridSearchCV
from sklearn.neighbors import KNeighborsClassifier
from sklearn.pipeline import Pipeline

from lakehouse.common.config import load_config
from lakehouse.common.logging import get_logger
from lakehouse.ml.evaluate import compute_metrics, log_report
from lakehouse.ml.features import load_splits
from lakehouse.ml.tracking import init_mlflow, registered_model_name, standard_tags

log = get_logger(__name__)


def _pipelines() -> dict[str, tuple[Pipeline, dict]]:
    tfidf = TfidfVectorizer(max_features=50_000, ngram_range=(1, 2), min_df=3, sublinear_tf=True)
    return {
        "knn": (
            Pipeline([("tfidf", tfidf), ("clf", KNeighborsClassifier(metric="cosine"))]),
            {"clf__n_neighbors": [5, 11, 21], "clf__weights": ["distance"]},
        ),
        "random_forest": (
            Pipeline(
                [
                    ("tfidf", tfidf),
                    ("clf", RandomForestClassifier(random_state=42, n_jobs=-1)),
                ]
            ),
            {"clf__n_estimators": [200, 400], "clf__max_depth": [None, 40]},
        ),
    }


def run() -> None:
    cfg = load_config()
    init_mlflow(cfg)
    tags = standard_tags(cfg)
    splits = load_splits(cfg)
    text_col, label_col = cfg.ml.text_col, cfg.ml.label_col

    x_train, y_train = splits["train"][text_col], splits["train"][label_col]
    x_val, y_val = splits["val"][text_col], splits["val"][label_col]
    x_test, y_test = splits["test"][text_col], splits["test"][label_col]

    best_name, best_f1, best_run_id = "", -1.0, ""
    for name, (pipeline, grid) in _pipelines().items():
        with mlflow.start_run(run_name=f"classical-{name}", tags=tags) as run:
            search = GridSearchCV(pipeline, grid, scoring="f1_macro", cv=3, n_jobs=-1)
            search.fit(x_train, y_train)

            val_pred = search.predict(x_val)
            val_metrics = compute_metrics(y_val.to_numpy(), val_pred)
            test_pred = search.predict(x_test)
            test_proba = search.predict_proba(x_test)[:, 1]
            test_metrics = compute_metrics(y_test.to_numpy(), test_pred, test_proba)
            log_report(name, y_test.to_numpy(), test_pred)

            mlflow.log_params(search.best_params_)
            mlflow.log_metrics({f"val_{k}": v for k, v in val_metrics.items()})
            mlflow.log_metrics({f"test_{k}": v for k, v in test_metrics.items()})
            mlflow.sklearn.log_model(search.best_estimator_, artifact_path="model")

            if val_metrics["f1_macro"] > best_f1:
                best_name, best_f1, best_run_id = name, val_metrics["f1_macro"], run.info.run_id

    log.info("Champion classical model: %s (val f1_macro=%.4f)", best_name, best_f1)
    mlflow.register_model(f"runs:/{best_run_id}/model", registered_model_name(cfg, "classical"))


if __name__ == "__main__":
    run()
