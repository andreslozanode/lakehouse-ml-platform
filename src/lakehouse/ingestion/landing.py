"""Landing-zone normalization with Polars.

Raw vendor formats (Kaggle CSV, Reddit NDJSON) are normalized into typed
Parquet with a stable schema + batch manifest, so Bronze only ever reads one
well-known layout. Polars is used here because these are single-node,
pre-cluster files where its lazy engine is dramatically faster than Spark.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from lakehouse.common.config import load_config
from lakehouse.common.logging import get_logger

log = get_logger(__name__)

REVIEW_SCHEMA = {
    "Id": pl.Int64,
    "ProductId": pl.Utf8,
    "UserId": pl.Utf8,
    "Score": pl.Int64,
    "Time": pl.Int64,
    "Summary": pl.Utf8,
    "Text": pl.Utf8,
}


def _new_batch_id() -> str:
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"


def _write_manifest(out_dir: Path, batch_id: str, source: str, rows: int) -> None:
    manifest = {
        "batch_id": batch_id,
        "source": source,
        "rows": rows,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (out_dir / "_MANIFEST.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def normalize_reviews() -> Path | None:
    cfg = load_config()
    raw_dir = Path(cfg.paths.landing) / "kaggle" / cfg.sources.kaggle.dataset.replace("/", "__")
    csv_files = sorted(raw_dir.glob("*.csv"))
    if not csv_files:
        log.warning("No Kaggle CSV found under %s; run kaggle ingestion first.", raw_dir)
        return None

    batch_id = _new_batch_id()
    out_dir = Path(cfg.paths.landing) / "normalized" / "reviews" / f"batch_id={batch_id}"
    out_dir.mkdir(parents=True, exist_ok=True)

    lf = (
        pl.scan_csv(csv_files[0], schema_overrides=REVIEW_SCHEMA, ignore_errors=True)
        .select(
            pl.col("Id").alias("review_id"),
            pl.col("ProductId").alias("product_id"),
            pl.col("UserId").alias("user_id"),
            pl.col("Score").alias("score"),
            pl.from_epoch(pl.col("Time"), time_unit="s").alias("review_ts"),
            pl.col("Summary").alias("summary"),
            pl.col("Text").alias("text"),
        )
        .with_columns(pl.lit(batch_id).alias("_batch_id"))
    )
    df = lf.collect(streaming=True)
    df.write_parquet(out_dir / "part-000.parquet", compression="zstd")
    _write_manifest(out_dir, batch_id, "kaggle_reviews", df.height)
    log.info("reviews batch %s: %d rows -> %s", batch_id, df.height, out_dir)
    return out_dir


def normalize_reddit() -> Path | None:
    cfg = load_config()
    raw_root = Path(cfg.paths.landing) / "reddit"
    files = sorted(raw_root.rglob("*.ndjson"))
    if not files:
        log.warning("No Reddit NDJSON found under %s; run reddit ingestion first.", raw_root)
        return None

    batch_id = _new_batch_id()
    out_dir = Path(cfg.paths.landing) / "normalized" / "reddit_posts" / f"batch_id={batch_id}"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = (
        pl.scan_ndjson(files, infer_schema_length=2000)
        .select(
            pl.col("id").alias("post_id"),
            pl.col("subreddit"),
            pl.col("author"),
            pl.col("title"),
            pl.col("selftext"),
            pl.col("score").cast(pl.Int64),
            pl.col("num_comments").cast(pl.Int64),
            pl.col("upvote_ratio").cast(pl.Float64),
            pl.from_epoch(pl.col("created_utc").cast(pl.Int64), time_unit="s").alias("created_ts"),
        )
        .with_columns(pl.lit(batch_id).alias("_batch_id"))
        .collect()
    )
    df.write_parquet(out_dir / "part-000.parquet", compression="zstd")
    _write_manifest(out_dir, batch_id, "reddit_posts", df.height)
    log.info("reddit batch %s: %d rows -> %s", batch_id, df.height, out_dir)
    return out_dir


def run_all() -> None:
    normalize_reviews()
    normalize_reddit()


if __name__ == "__main__":
    run_all()
