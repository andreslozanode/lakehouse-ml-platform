"""Gold -> Snowflake delivery for BI consumers.

Pattern: stage into a transient table with write_pandas, then MERGE into the
final table inside one transaction (idempotent re-runs, no partial loads).
"""

from __future__ import annotations

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from lakehouse.common.config import Config, load_config
from lakehouse.common.logging import get_logger
from lakehouse.common.secrets import resolve_secret
from lakehouse.common.spark import get_spark, table_path

log = get_logger(__name__)

MERGE_KEYS: dict[str, list[str]] = {
    "gold_reviews_daily": ["REVIEW_DATE", "PRODUCT_ID"],
    "gold_subreddit_engagement": ["CREATED_DATE", "SUBREDDIT"],
    "gold_orders_enriched": ["ORDER_ID"],
}


def _connect(cfg: Config):
    sf = cfg.delivery.snowflake
    return snowflake.connector.connect(
        account=sf.account,
        user=sf.user,
        password=resolve_secret("SNOWFLAKE_PASSWORD"),
        role=sf.role,
        warehouse=sf.warehouse,
        database=sf.database,
        schema=sf["schema"],
    )


def _merge_sql(target: str, staging: str, columns: list[str], keys: list[str]) -> str:
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    update = ", ".join(f"t.{c} = s.{c}" for c in columns if c not in keys)
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join(f"s.{c}" for c in columns)
    return (
        f"MERGE INTO {target} t USING {staging} s ON {on} "
        f"WHEN MATCHED THEN UPDATE SET {update} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


def load_table(conn, cfg: Config, table: str) -> None:
    spark = get_spark(cfg)
    sdf = spark.read.format("delta").load(table_path(cfg, "gold", table))
    pdf: pd.DataFrame = sdf.toPandas()
    pdf.columns = [c.upper() for c in pdf.columns]
    if pdf.empty:
        log.warning("[snowflake] %s is empty; skipping.", table)
        return

    target = table.upper()
    staging = f"{target}_STG"
    cursor = conn.cursor()
    try:
        cursor.execute("BEGIN")
        cursor.execute(f"CREATE OR REPLACE TRANSIENT TABLE {staging} LIKE {target}")
        write_pandas(conn, pdf, staging, auto_create_table=False, overwrite=False)
        cursor.execute(_merge_sql(target, staging, list(pdf.columns), MERGE_KEYS[table]))
        cursor.execute(f"DROP TABLE IF EXISTS {staging}")
        cursor.execute("COMMIT")
        log.info("[snowflake] %s: %d rows merged.", target, len(pdf))
    except Exception:
        cursor.execute("ROLLBACK")
        raise
    finally:
        cursor.close()


def run() -> None:
    cfg = load_config()
    conn = _connect(cfg)
    try:
        for table in cfg.delivery.snowflake.tables:
            load_table(conn, cfg, table)
    finally:
        conn.close()


if __name__ == "__main__":
    run()
