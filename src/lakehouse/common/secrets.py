"""Cloud-aware secrets resolver.

Resolution order:
  1. Process environment variable (12-factor default, used by CI/CD)
  2. Databricks secret scope (when running on a Databricks cluster)
  3. AWS Secrets Manager (when boto3 + credentials are available)

Usage:
    resolve_secret("SNOWFLAKE_PASSWORD", scope="lakehouse")
"""

from __future__ import annotations

import json
import os

from lakehouse.common.logging import get_logger

log = get_logger(__name__)


def _from_databricks(key: str, scope: str) -> str | None:
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            return None
        return DBUtils(spark).secrets.get(scope=scope, key=key)
    except Exception:
        return None


def _from_aws(key: str, secret_id: str) -> str | None:
    try:
        import boto3

        client = boto3.client("secretsmanager")
        payload = client.get_secret_value(SecretId=secret_id)["SecretString"]
        return json.loads(payload).get(key)
    except Exception:
        return None


def resolve_secret(key: str, scope: str = "lakehouse", required: bool = True) -> str:
    value = os.environ.get(key) or _from_databricks(key, scope) or _from_aws(key, scope)
    if value:
        return value
    if required:
        raise RuntimeError(f"Secret '{key}' not found in env, Databricks scope or AWS SM.")
    log.warning("Optional secret '%s' not found; continuing with empty value.", key)
    return ""
