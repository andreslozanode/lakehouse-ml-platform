"""Feature preparation: gold_text_features -> stratified train/val/test splits.

Splits are materialized as Parquet artifacts so every trainer (sklearn, torch,
tensorflow) consumes exactly the same data, making metrics comparable.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from sklearn.model_selection import train_test_split

from lakehouse.common.config import Config, load_config
from lakehouse.common.logging import get_logger

log = get_logger(__name__)


def _load_gold(cfg: Config) -> pd.DataFrame:
    from lakehouse.common.spark import get_spark, table_path

    spark = get_spark(cfg)
    sdf = spark.read.format("delta").load(table_path(cfg, "gold", "gold_text_features"))
    sample_rows = int(cfg.ml.sample_rows)
    if sample_rows > 0:
        total = sdf.count()
        if total > sample_rows:
            sdf = sdf.sample(fraction=sample_rows / total, seed=int(cfg.ml.random_state))
    return sdf.select("id", cfg.ml.text_col, cfg.ml.label_col).toPandas()


def make_splits(cfg: Config | None = None) -> dict[str, Path]:
    cfg = cfg or load_config()
    out_dir = Path(cfg.paths.meta) / "ml_splits"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = _load_gold(cfg).dropna(subset=[cfg.ml.text_col, cfg.ml.label_col])
    seed, label = int(cfg.ml.random_state), cfg.ml.label_col

    train_val, test = train_test_split(
        df, test_size=float(cfg.ml.test_size), stratify=df[label], random_state=seed
    )
    rel_val = float(cfg.ml.val_size) / (1.0 - float(cfg.ml.test_size))
    train, val = train_test_split(
        train_val, test_size=rel_val, stratify=train_val[label], random_state=seed
    )

    paths: dict[str, Path] = {}
    for name, part in {"train": train, "val": val, "test": test}.items():
        path = out_dir / f"{name}.parquet"
        part.reset_index(drop=True).to_parquet(path, index=False)
        paths[name] = path
        log.info("split %-5s -> %6d rows (%s)", name, len(part), path)
    return paths


def load_splits(cfg: Config | None = None) -> dict[str, pd.DataFrame]:
    cfg = cfg or load_config()
    out_dir = Path(cfg.paths.meta) / "ml_splits"
    if not (out_dir / "train.parquet").exists():
        make_splits(cfg)
    return {name: pd.read_parquet(out_dir / f"{name}.parquet") for name in ("train", "val", "test")}


if __name__ == "__main__":
    make_splits()
