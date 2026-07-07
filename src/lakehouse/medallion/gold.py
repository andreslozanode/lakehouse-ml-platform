"""Gold layer: business aggregates + the ML feature table.

Tables produced:
  gold_reviews_daily          - product-day review metrics for BI.
  gold_subreddit_engagement   - daily engagement per subreddit.
  gold_orders_enriched        - CDC orders joined with customers (serving-ready).
  gold_text_features          - unified (text, label) feature table for training.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse.common.config import Config, load_config
from lakehouse.common.logging import get_logger
from lakehouse.common.spark import get_spark, table_path

log = get_logger(__name__)


def _write(df, cfg: Config, name: str, partition_by: list[str] | None = None) -> None:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(table_path(cfg, "gold", name))
    log.info("gold.%s rebuilt.", name)


def build_reviews_daily(spark: SparkSession, cfg: Config) -> None:
    df = spark.read.format("delta").load(table_path(cfg, "silver", "reviews"))
    agg = df.groupBy("review_date", "product_id").agg(
        F.count("*").alias("review_count"),
        F.round(F.avg("score"), 3).alias("avg_score"),
        F.sum(F.when(F.col("sentiment_label") == "positive", 1).otherwise(0)).alias("positives"),
        F.sum(F.when(F.col("sentiment_label") == "negative", 1).otherwise(0)).alias("negatives"),
        F.round(F.avg(F.length("text_clean")), 1).alias("avg_text_len"),
    )
    _write(agg, cfg, "gold_reviews_daily", partition_by=["review_date"])


def build_subreddit_engagement(spark: SparkSession, cfg: Config) -> None:
    df = spark.read.format("delta").load(table_path(cfg, "silver", "reddit_posts"))
    agg = df.groupBy("created_date", "subreddit").agg(
        F.count("*").alias("posts"),
        F.round(F.avg("score"), 2).alias("avg_score"),
        F.round(F.avg("num_comments"), 2).alias("avg_comments"),
        F.round(F.avg("upvote_ratio"), 4).alias("avg_upvote_ratio"),
        F.sum(F.col("is_engaged").cast("int")).alias("engaged_posts"),
    )
    _write(agg, cfg, "gold_subreddit_engagement", partition_by=["created_date"])


def build_orders_enriched(spark: SparkSession, cfg: Config) -> None:
    try:
        orders = spark.read.format("delta").load(table_path(cfg, "silver", "cdc_orders"))
        customers = spark.read.format("delta").load(table_path(cfg, "silver", "cdc_customers"))
    except Exception:
        log.warning("CDC silver tables not present yet; skipping gold_orders_enriched.")
        return
    enriched = (
        orders.alias("o")
        .join(customers.alias("c"), "customer_id", "left")
        .select(
            "o.order_id",
            "o.customer_id",
            "c.full_name",
            "c.segment",
            "c.country",
            "o.status",
            "o.amount",
            "o.currency",
            "o.updated_at",
        )
        .withColumn("order_date", F.to_date("updated_at"))
    )
    _write(enriched, cfg, "gold_orders_enriched", partition_by=["order_date"])


def build_text_features(spark: SparkSession, cfg: Config) -> None:
    """Binary sentiment feature table (neutral rows excluded on purpose)."""
    df = spark.read.format("delta").load(table_path(cfg, "silver", "reviews"))
    features = (
        df.filter(F.col("sentiment_label") != "neutral")
        .select(
            F.col("review_id").alias("id"),
            F.col("text_clean").alias(cfg.ml.text_col),
            F.when(F.col("sentiment_label") == "positive", 1).otherwise(0).alias(cfg.ml.label_col),
            "review_date",
        )
        .filter(F.length(cfg.ml.text_col) >= 10)
    )
    _write(features, cfg, "gold_text_features")


def run() -> None:
    cfg = load_config()
    spark = get_spark(cfg)
    build_reviews_daily(spark, cfg)
    build_subreddit_engagement(spark, cfg)
    build_orders_enriched(spark, cfg)
    build_text_features(spark, cfg)


if __name__ == "__main__":
    run()
