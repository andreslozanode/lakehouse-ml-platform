"""Bronze layer: raw-but-typed append-only Delta tables.

Contract:
  * Source of truth for replays; no business transformations.
  * Idempotent by `_batch_id` (replace-where on re-run of the same batch).
  * Lineage columns: _batch_id, _ingest_ts, _source.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.common.config import load_config
from lakehouse.common.logging import get_logger
from lakehouse.common.spark import get_spark, table_path

log = get_logger(__name__)

_ENTITIES = ("reviews", "reddit_posts")


def _with_lineage(df: DataFrame, source: str) -> DataFrame:
    return df.withColumn("_ingest_ts", F.current_timestamp()).withColumn("_source", F.lit(source))


def _write_idempotent(df: DataFrame, target: str) -> None:
    """Append with batch-level idempotency: same _batch_id can be safely replayed."""
    batch_ids = [row["_batch_id"] for row in df.select("_batch_id").distinct().collect()]
    predicate = "_batch_id IN ({})".format(",".join(f"'{b}'" for b in batch_ids))
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .option("mergeSchema", "true")
        .save(target)
    )


def run() -> None:
    cfg = load_config()
    spark = get_spark(cfg)

    for entity in _ENTITIES:
        src = f"{cfg.paths.landing}/normalized/{entity}"
        try:
            df = spark.read.parquet(src)
        except Exception:
            log.warning("No normalized landing data for '%s' at %s; skipping.", entity, src)
            continue
        target = table_path(cfg, "bronze", entity)
        _write_idempotent(_with_lineage(df, entity), target)
        log.info("bronze.%s <- %d rows (%s)", entity, df.count(), target)


if __name__ == "__main__":
    run()
