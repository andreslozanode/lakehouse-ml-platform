import pytest

from lakehouse.medallion.silver import clean_text, dedupe_latest

pytestmark = pytest.mark.spark


def test_dedupe_latest_keeps_most_recent(spark):
    df = spark.createDataFrame(
        [(1, "old", 100), (1, "new", 200), (2, "only", 50)],
        "id int, payload string, ts long",
    )
    out = dedupe_latest(df, ["id"], "ts")
    rows = {r["id"]: r["payload"] for r in out.collect()}
    assert rows == {1: "new", 2: "only"}


def test_clean_text_strips_html_and_urls(spark):
    df = spark.createDataFrame(
        [("<b>Great</b> product http://x.co &amp; MORE   spaces",)], "text string"
    )
    result = df.select(clean_text("text").alias("c")).first()["c"]
    assert "<b>" not in result and "http" not in result
    assert result == "great product more spaces"


def test_clean_text_handles_null(spark):
    df = spark.createDataFrame([(None,)], "text string")
    assert df.select(clean_text("text").alias("c")).first()["c"] == ""
