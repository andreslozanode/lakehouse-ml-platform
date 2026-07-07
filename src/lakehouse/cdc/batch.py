"""Batch CDC: high-watermark incremental extraction from Postgres via JDBC.

Watermarks are persisted in a Delta control table (`_meta/watermarks`) so runs
are restartable and exactly-once at the row level (MERGE on primary key).
Downstream propagation can use Delta Change Data Feed on the silver targets.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.cdc.merge import merge_changes
from lakehouse.common.config import Config, load_config
from lakehouse.common.logging import get_logger
from lakehouse.common.spark import get_spark, table_path

log = get_logger(__name__)

_EPOCH = "1970-01-01 00:00:00"


def _watermark_path(cfg: Config) -> str:
    return f"{cfg.paths.meta}/watermarks"


def read_watermark(spark: SparkSession, cfg: Config, table: str) -> str:
    try:
        df = spark.read.format("delta").load(_watermark_path(cfg))
        row = (
            df.filter(F.col("table") == table)
            .orderBy(F.col("updated_at").desc())
            .select("watermark")
            .first()
        )
        return row["watermark"] if row else _EPOCH
    except Exception:
        return _EPOCH


def write_watermark(spark: SparkSession, cfg: Config, table: str, watermark: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    df = spark.createDataFrame(
        [(table, watermark, now)], "table string, watermark string, updated_at string"
    )
    df.write.format("delta").mode("append").save(_watermark_path(cfg))


def extract_incremental(spark: SparkSession, cfg: Config, table: str, watermark: str) -> DataFrame:
    pg = cfg.cdc.postgres
    wm_col = cfg.cdc.batch.watermark_column
    query = f"(SELECT * FROM public.{table} WHERE {wm_col} > '{watermark}'::timestamptz) AS src"
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{pg.host}:{pg.port}/{pg.database}")
        .option("dbtable", query)
        .option("user", pg.user)
        .option("password", pg.password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
    )


def sync_table(spark: SparkSession, cfg: Config, table: str) -> int:
    wm_col = cfg.cdc.batch.watermark_column
    keys = list(cfg.cdc.batch.merge_keys[table])
    watermark = read_watermark(spark, cfg, table)

    incoming = extract_incremental(spark, cfg, table, watermark)
    count = incoming.count()
    if count == 0:
        log.info("[batch-cdc.%s] no changes since %s.", table, watermark)
        return 0

    changes = incoming.withColumn("_op", F.lit("u"))  # JDBC snapshot rows are upserts
    merge_changes(spark, changes, table_path(cfg, "silver", f"cdc_{table}"), keys)

    new_wm = incoming.agg(F.max(wm_col).cast("string")).first()[0]
    write_watermark(spark, cfg, table, new_wm)
    log.info("[batch-cdc.%s] merged %d rows; watermark -> %s.", table, count, new_wm)
    return count


def run() -> None:
    cfg = load_config()
    spark = get_spark(cfg)
    for fq_table in cfg.cdc.tables:
        sync_table(spark, cfg, fq_table.split(".")[-1])


if __name__ == "__main__":
    run()
