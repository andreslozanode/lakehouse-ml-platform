import os

from lakehouse.common.config import load_config


def test_env_interpolation_with_default(monkeypatch):
    load_config.cache_clear()
    monkeypatch.delenv("LAKEHOUSE_ROOT", raising=False)
    cfg = load_config("dev")
    assert cfg.paths.root == "./data"
    assert cfg.paths.bronze.endswith("/bronze")


def test_env_overlay_overrides_base(monkeypatch):
    load_config.cache_clear()
    monkeypatch.setenv("LAKEHOUSE_ROOT", "/tmp/x")
    cfg = load_config("prod")
    assert cfg.environment == "prod"
    assert cfg.spark.shuffle_partitions == 200
    assert cfg.paths.silver == "/tmp/x/silver"
    load_config.cache_clear()
    os.environ.pop("LAKEHOUSE_ROOT", None)
