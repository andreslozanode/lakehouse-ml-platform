"""SparkSession factory with Delta Lake enabled.

On Databricks the runtime session is reused untouched; locally a Delta-enabled
session is built with sane defaults (AQE, small shuffle partitions).
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from lakehouse.common.config import Config, load_config


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(cfg: Config | None = None) -> SparkSession:
    cfg = cfg or load_config()
    active = SparkSession.getActiveSession()
    if active is not None and is_databricks():
        return active

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(cfg.spark.app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", str(cfg.spark.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
    )
    for key, value in dict(cfg.spark.get("extra_conf", {})).items():
        builder = builder.config(key, str(value))

    # spark-sql-kafka is required by the streaming CDC job when run locally.
    packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"]
    return configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()


def table_path(cfg: Config, layer: str, table: str) -> str:
    """Path-based Delta location for a table within a medallion layer."""
    return f"{cfg.paths[layer]}/{table}"
