import pytest

from lakehouse.cdc.merge import latest_change_per_key, merge_changes

pytestmark = pytest.mark.spark


def test_latest_change_per_key(spark):
    df = spark.createDataFrame(
        [(1, "created", 100, "u"), (1, "paid", 300, "u"), (1, "shipped", 200, "u")],
        "order_id int, status string, _event_ts_ms long, _op string",
    )
    out = latest_change_per_key(df, ["order_id"], "_event_ts_ms")
    assert out.count() == 1
    assert out.first()["status"] == "paid"


def test_merge_applies_upserts_and_deletes(spark, tmp_path):
    target = str(tmp_path / "orders_delta")

    seed = spark.createDataFrame(
        [(1, "created", "u"), (2, "created", "u")], "order_id int, status string, _op string"
    )
    merge_changes(spark, seed, target, ["order_id"])

    changes = spark.createDataFrame(
        [(1, "paid", "u"), (2, "created", "d"), (3, "created", "c")],
        "order_id int, status string, _op string",
    )
    merge_changes(spark, changes, target, ["order_id"])

    result = {r["order_id"]: r["status"] for r in spark.read.format("delta").load(target).collect()}
    assert result == {1: "paid", 3: "created"}
