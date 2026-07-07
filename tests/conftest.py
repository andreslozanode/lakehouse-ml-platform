"""Shared fixtures: local Delta-enabled Spark session + isolated lakehouse root."""

from __future__ import annotations

import os

import pytest


@pytest.fixture(scope="session")
def lakehouse_root(tmp_path_factory) -> str:
    root = tmp_path_factory.mktemp("lakehouse")
    os.environ["LAKEHOUSE_ROOT"] = str(root)
    os.environ["LAKEHOUSE_ENV"] = "dev"
    return str(root)


@pytest.fixture(scope="session")
def spark(lakehouse_root):
    from lakehouse.common.config import load_config
    from lakehouse.common.spark import get_spark

    load_config.cache_clear()
    session = get_spark(load_config())
    yield session
    session.stop()
