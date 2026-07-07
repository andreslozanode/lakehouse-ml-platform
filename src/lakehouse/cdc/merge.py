"""Pure MERGE semantics shared by streaming and batch CDC (unit-testable)."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def latest_change_per_key(df: DataFrame, keys: list[str], version_col: str) -> DataFrame:
    """Collapse a micro-batch of changes to the last event per primary key."""
    window = Window.partitionBy(*keys).orderBy(F.col(version_col).desc())
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def merge_changes(
    spark: SparkSession,
    changes: DataFrame,
    target_path: str,
    keys: list[str],
    op_col: str = "_op",
) -> None:
    """Apply Debezium-style changes (c/u/r = upsert, d = delete) to a Delta target."""
    payload_cols = [c for c in changes.columns if c != op_col]

    if not DeltaTable.isDeltaTable(spark, target_path):
        changes.filter(F.col(op_col) != "d").select(*payload_cols).write.format("delta").mode(
            "overwrite"
        ).save(target_path)
        return

    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    set_map = {c: f"s.{c}" for c in payload_cols}
    (
        DeltaTable.forPath(spark, target_path)
        .alias("t")
        .merge(changes.alias("s"), cond)
        .whenMatchedDelete(condition=f"s.{op_col} = 'd'")
        .whenMatchedUpdate(condition=f"s.{op_col} != 'd'", set=set_map)
        .whenNotMatchedInsert(condition=f"s.{op_col} != 'd'", values=set_map)
        .execute()
    )
