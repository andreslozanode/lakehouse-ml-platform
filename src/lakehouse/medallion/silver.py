"""Silver layer: cleaned, deduplicated, conformed entities with DQ gates."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse.common.config import Config, load_config
from lakehouse.common.logging import get_logger
from lakehouse.common.spark import get_spark, table_path
from lakehouse.quality import Action, Expectation, ExpectationEngine

log = get_logger(__name__)


def dedupe_latest(df: DataFrame, keys: list[str], order_col: str) -> DataFrame:
    """Keep the most recent record per natural key (deterministic tie-break)."""
    window = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def clean_text(col: str) -> F.Column:
    """Normalize free text: strip HTML entities/URLs, collapse whitespace, lowercase."""
    c = F.coalesce(F.col(col), F.lit(""))
    c = F.regexp_replace(c, r"<[^>]+>", " ")
    c = F.regexp_replace(c, r"http\S+", " ")
    c = F.regexp_replace(c, r"&\w+;", " ")
    c = F.regexp_replace(c, r"\s+", " ")
    return F.lower(F.trim(c))


def _merge_upsert(spark: SparkSession, df: DataFrame, target: str, keys: list[str]) -> None:
    if not DeltaTable.isDeltaTable(spark, target):
        df.write.format("delta").mode("overwrite").save(target)
        return
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        DeltaTable.forPath(spark, target)
        .alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def build_reviews(spark: SparkSession, cfg: Config) -> None:
    df = spark.read.format("delta").load(table_path(cfg, "bronze", "reviews"))
    df = dedupe_latest(df, ["review_id"], "_ingest_ts")
    df = (
        df.withColumn("text_clean", clean_text("text"))
        .withColumn("summary_clean", clean_text("summary"))
        .withColumn("review_date", F.to_date("review_ts"))
        .withColumn(
            "sentiment_label",
            F.when(F.col("score") >= 4, F.lit("positive"))
            .when(F.col("score") <= 2, F.lit("negative"))
            .otherwise(F.lit("neutral")),
        )
    )
    rules = [
        Expectation("pk_not_null", "review_id IS NOT NULL", Action.FAIL),
        Expectation("score_in_range", "score BETWEEN 1 AND 5", Action.DROP),
        Expectation("non_empty_text", "length(text_clean) > 0", Action.DROP),
        Expectation("ts_not_future", "review_ts <= current_timestamp()", Action.WARN),
    ]
    df = ExpectationEngine(cfg, "silver.reviews").apply(df, rules)
    _merge_upsert(spark, df, table_path(cfg, "silver", "reviews"), ["review_id"])
    log.info("silver.reviews upserted.")


def build_reddit_posts(spark: SparkSession, cfg: Config) -> None:
    df = spark.read.format("delta").load(table_path(cfg, "bronze", "reddit_posts"))
    df = dedupe_latest(df, ["post_id"], "_ingest_ts")
    df = (
        df.withColumn("title_clean", clean_text("title"))
        .withColumn("body_clean", clean_text("selftext"))
        .withColumn("created_date", F.to_date("created_ts"))
        .withColumn("is_engaged", (F.col("num_comments") >= 5) | (F.col("score") >= 20))
    )
    rules = [
        Expectation("pk_not_null", "post_id IS NOT NULL", Action.FAIL),
        Expectation("subreddit_not_null", "subreddit IS NOT NULL", Action.DROP),
        Expectation("score_not_negative_extreme", "score > -1000", Action.WARN),
    ]
    df = ExpectationEngine(cfg, "silver.reddit_posts").apply(df, rules)
    _merge_upsert(spark, df, table_path(cfg, "silver", "reddit_posts"), ["post_id"])
    log.info("silver.reddit_posts upserted.")


def run() -> None:
    cfg = load_config()
    spark = get_spark(cfg)
    build_reviews(spark, cfg)
    build_reddit_posts(spark, cfg)


if __name__ == "__main__":
    run()
