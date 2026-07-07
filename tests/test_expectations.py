import pytest

from lakehouse.common.config import load_config
from lakehouse.quality import Action, Expectation, ExpectationEngine
from lakehouse.quality.expectations import DataQualityError

pytestmark = pytest.mark.spark


@pytest.fixture()
def sample_df(spark):
    return spark.createDataFrame(
        [(1, 5, "great"), (2, 3, "ok"), (3, 99, "broken"), (None, 4, "no-key")],
        "id int, score int, text string",
    )


def test_drop_removes_only_violations(spark, sample_df, lakehouse_root):
    cfg = load_config()
    engine = ExpectationEngine(cfg, "test.drop")
    rule = Expectation("score_range", "score BETWEEN 1 AND 5", Action.DROP)
    out = engine.apply(sample_df, [rule])
    assert out.count() == 3
    assert out.filter("score = 99").count() == 0


def test_warn_keeps_all_rows(spark, sample_df, lakehouse_root):
    cfg = load_config()
    engine = ExpectationEngine(cfg, "test.warn")
    rule = Expectation("score_range", "score BETWEEN 1 AND 5", Action.WARN)
    out = engine.apply(sample_df, [rule])
    assert out.count() == sample_df.count()


def test_fail_raises_above_threshold(spark, sample_df, lakehouse_root):
    cfg = load_config()
    engine = ExpectationEngine(cfg, "test.fail")
    with pytest.raises(DataQualityError):
        engine.apply(sample_df, [Expectation("pk_not_null", "id IS NOT NULL", Action.FAIL)])
