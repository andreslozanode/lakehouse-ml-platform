"""Real-time CDC: Kafka (Debezium full envelope) -> Delta silver via MERGE.

Design decisions:
  * The FULL Debezium envelope (before/after/op/ts_ms/source) is consumed
    instead of flattening with ExtractNewRecordState, so deletes and source
    LSN ordering remain available to the consumer.
  * foreachBatch collapses each micro-batch to the latest event per key before
    merging, guaranteeing idempotent, ordered upserts.
  * `--once` uses availableNow for backfills; default is continuous with a
    processing-time trigger.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from lakehouse.cdc.merge import latest_change_per_key, merge_changes
from lakehouse.common.config import Config, load_config
from lakehouse.common.logging import get_logger
from lakehouse.common.spark import get_spark, table_path

log = get_logger(__name__)

TABLE_SCHEMAS: dict[str, StructType] = {
    "customers": StructType(
        [
            StructField("customer_id", LongType()),
            StructField("full_name", StringType()),
            StructField("email", StringType()),
            StructField("segment", StringType()),
            StructField("country", StringType()),
            StructField("updated_at", StringType()),
        ]
    ),
    "orders": StructType(
        [
            StructField("order_id", LongType()),
            StructField("customer_id", LongType()),
            StructField("status", StringType()),
            StructField("amount", StringType()),
            StructField("currency", StringType()),
            StructField("updated_at", StringType()),
        ]
    ),
}
MERGE_KEYS = {"customers": ["customer_id"], "orders": ["order_id"]}


def _envelope_schema(row_schema: StructType) -> StructType:
    return StructType(
        [
            StructField("before", row_schema),
            StructField("after", row_schema),
            StructField("op", StringType()),
            StructField("ts_ms", LongType()),
            StructField(
                "source",
                StructType([StructField("lsn", LongType()), StructField("table", StringType())]),
            ),
        ]
    )


def _parse(df: DataFrame, table: str) -> DataFrame:
    """Unwrap the full Debezium envelope into typed change rows."""
    schema = _envelope_schema(TABLE_SCHEMAS[table])
    parsed = df.select(
        F.from_json(
            F.col("value").cast("string"), StructType([StructField("payload", schema)])
        ).alias("j")
    ).select("j.payload.*")

    # For deletes Debezium only populates `before`; use it as the row payload.
    row = F.when(F.col("op") == "d", F.col("before")).otherwise(F.col("after"))
    out = (
        parsed.withColumn("row", row)
        .select("row.*", F.col("op").alias("_op"), F.col("ts_ms").alias("_event_ts_ms"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )
    if table == "orders":
        out = out.withColumn("amount", F.col("amount").cast("decimal(12,2)"))
    return out


def _make_upsert(cfg: Config, table: str):
    target = table_path(cfg, "silver", f"cdc_{table}")
    keys = MERGE_KEYS[table]

    def upsert(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        deduped = latest_change_per_key(batch_df, keys, "_event_ts_ms").drop("_event_ts_ms")
        merge_changes(batch_df.sparkSession, deduped, target, keys)
        log.info("[cdc.%s] micro-batch %d merged.", table, batch_id)

    return upsert


def run(once: bool = False) -> None:
    cfg = load_config()
    spark = get_spark(cfg)
    queries = []

    for table in TABLE_SCHEMAS:
        topic = f"{cfg.cdc.topic_prefix}.public.{table}"
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", cfg.cdc.kafka_bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        writer = (
            _parse(raw, table)
            .writeStream.foreachBatch(_make_upsert(cfg, table))
            .option("checkpointLocation", f"{cfg.paths.checkpoints}/cdc_{table}")
            .queryName(f"cdc_{table}")
        )
        writer = (
            writer.trigger(availableNow=True)
            if once
            else writer.trigger(processingTime="30 seconds")
        )
        queries.append(writer.start())
        log.info("CDC stream started for %s (topic=%s).", table, topic)

    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="availableNow backfill mode")
    run(once=parser.parse_args().once)
