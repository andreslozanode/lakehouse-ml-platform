"""Single CLI entrypoint used by Makefile, Airflow and Databricks jobs.

python -m lakehouse.cli <stage>
stages: ingest | bronze | silver | gold | cdc-batch | cdc-stream | deliver | pipeline
"""

from __future__ import annotations

import argparse
import sys

from lakehouse.common.logging import get_logger

log = get_logger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse")
    parser.add_argument(
        "stage",
        choices=[
            "ingest",
            "bronze",
            "silver",
            "gold",
            "cdc-batch",
            "cdc-stream",
            "deliver",
            "pipeline",
        ],
    )
    parser.add_argument("--once", action="store_true", help="cdc-stream: availableNow mode")
    args = parser.parse_args(argv)

    if args.stage == "ingest":
        from lakehouse.ingestion import kaggle_client, landing, reddit_client

        kaggle_client.ingest_kaggle()
        reddit_client.ingest_reddit()
        landing.run_all()
    elif args.stage == "bronze":
        from lakehouse.medallion import bronze

        bronze.run()
    elif args.stage == "silver":
        from lakehouse.medallion import silver

        silver.run()
    elif args.stage == "gold":
        from lakehouse.medallion import gold

        gold.run()
    elif args.stage == "cdc-batch":
        from lakehouse.cdc import batch

        batch.run()
    elif args.stage == "cdc-stream":
        from lakehouse.cdc import streaming

        streaming.run(once=args.once)
    elif args.stage == "deliver":
        from lakehouse.delivery import snowflake_loader

        snowflake_loader.run()
    elif args.stage == "pipeline":
        for stage in ("ingest", "bronze", "silver", "gold"):
            main([stage])
    return 0


if __name__ == "__main__":
    sys.exit(main())
