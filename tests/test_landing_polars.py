"""Landing normalization contract tests (pure Polars, no Spark needed)."""

import polars as pl


def test_review_schema_projection():
    raw = pl.DataFrame(
        {
            "Id": [1],
            "ProductId": ["P1"],
            "UserId": ["U1"],
            "Score": [5],
            "Time": [1700000000],
            "Summary": ["good"],
            "Text": ["excellent product"],
        }
    )
    out = raw.select(
        pl.col("Id").alias("review_id"),
        pl.col("Score").alias("score"),
        pl.from_epoch(pl.col("Time"), time_unit="s").alias("review_ts"),
    )
    assert out.columns == ["review_id", "score", "review_ts"]
    assert out["review_ts"].dtype.is_temporal()
    assert out["review_ts"][0].year == 2023
