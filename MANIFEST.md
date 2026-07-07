# MANIFEST

Complete file inventory. Every artifact is deployable — no stubs or placeholders.

## Root
| File | Purpose |
|---|---|
| `README.md` | Project overview, architecture, quickstart, layout |
| `MANIFEST.md` | This inventory |
| `LICENSE` | MIT |
| `pyproject.toml` | Package metadata, extras (`spark/ml/serving/delivery/cdc/dev`), ruff/black/mypy/pytest config, `lakehouse` CLI entrypoint |
| `Makefile` | Developer targets: install, lint, test, pipeline stages, trainers, serve, local CDC stack |
| `docker-compose.yml` | Local stack: Postgres 16 (logical WAL + MLflow backend DB) + Kafka KRaft + Debezium Connect + MLflow Tracking Server (:5000) |
| `.env.example` | Every required environment variable, documented |
| `.gitignore` / `.pre-commit-config.yaml` | Hygiene: data/secrets exclusions; ruff, black, mypy, safety hooks |

## Configuration — `conf/`
| File | Purpose |
|---|---|
| `base.yaml` | Shared config: paths, sources, CDC, quality thresholds, ML hyperparams, Snowflake, Spark |
| `dev.yaml` / `qa.yaml` / `prod.yaml` | Per-environment overlays (roots, partitions, DQ thresholds, epochs) |

## Application — `src/lakehouse/`
| File | Purpose |
|---|---|
| `cli.py` | Single entrypoint for all stages (used by Make, Airflow, Databricks) |
| `common/config.py` | Layered YAML loader with `${VAR:default}` interpolation |
| `common/logging.py` | Structured logger factory |
| `common/secrets.py` | env → Databricks scope → AWS Secrets Manager resolver |
| `common/spark.py` | Delta-enabled SparkSession factory (Databricks-aware) |
| `ingestion/kaggle_client.py` | Kaggle Datasets REST download, idempotent via `.done` marker |
| `ingestion/reddit_client.py` | Reddit public JSON listings → NDJSON landing, rate-limited |
| `ingestion/landing.py` | Polars normalization → typed Parquet + batch `_MANIFEST.json` |
| `medallion/bronze.py` | Append-only Delta with lineage cols, `replaceWhere` batch idempotency |
| `medallion/silver.py` | Dedup, text cleansing, sentiment labeling, DQ gates, MERGE upserts |
| `medallion/gold.py` | `gold_reviews_daily`, `gold_subreddit_engagement`, `gold_orders_enriched`, `gold_text_features` |
| `quality/expectations.py` | Declarative DQ engine (warn/drop/fail) + Delta audit table |
| `cdc/merge.py` | Pure, unit-tested MERGE primitives (latest-per-key, upsert+delete) |
| `cdc/streaming.py` | Kafka → full Debezium envelope → foreachBatch MERGE (continuous or `--once`) |
| `cdc/batch.py` | JDBC high-watermark sync with Delta watermark control table |
| `delivery/snowflake_loader.py` | Transactional staged MERGE into Snowflake GOLD |
| `ml/features.py` | Gold → stratified train/val/test Parquet splits (shared by all trainers) |
| `ml/train_classical.py` | TF-IDF + KNN & RandomForest, GridSearchCV, MLflow, champion registration |
| `ml/train_bert_torch.py` | DistilBERT fine-tune: CUDA AMP, TF32, warmup, clipping, early stopping |
| `ml/train_tensorflow.py` | Keras BiLSTM: mixed_float16 on GPU, tf.data AUTOTUNE, callbacks |
| `ml/evaluate.py` | Unified metrics (acc, F1-macro, precision/recall, ROC-AUC) |
| `ml/tracking.py` | Centralized MLflow init: env-driven tracking/registry URIs, standard run tags (env, git SHA, dataset) |
| `ml/registry.py` | Champion/challenger promotion via MLflow aliases |
| `ml/serving/app.py` | FastAPI inference over MLflow `@champion` |
| `ml/serving/Dockerfile` | Multi-stage, non-root serving image with healthcheck |

## CDC infrastructure — `cdc/`, `scripts/`
| File | Purpose |
|---|---|
| `cdc/postgres/init.sql` | OLTP schema, REPLICA IDENTITY FULL, triggers, seed data |
| `cdc/debezium/register-postgres.json` | Connector config (pgoutput, full envelope, decimals as string) |
| `cdc/debezium/register.sh` | Idempotent create-or-update registration with readiness wait |
| `scripts/generate_oltp_traffic.py` | Mixed INSERT/UPDATE/DELETE workload generator |
| `scripts/bootstrap_local.sh` | One-shot venv + deps + stack + connector bootstrap |

## Orchestration — `airflow/dags/`
| File | Purpose |
|---|---|
| `medallion_daily_dag.py` | ingest → bronze → silver → gold → deliver (retries, SLA, TaskGroup) |
| `cdc_batch_dag.py` | Hourly watermark sync + gold refresh |
| `ml_training_dag.py` | Weekly: splits → parallel trainers → champion promotion |

## Deployment
| File | Purpose |
|---|---|
| `databricks.yml` | Asset Bundle: dev/qa/prod targets, wheel artifact, ETL/CDC/ML jobs (GPU task for BERT) |
| `infra/terraform/*.tf` | Versioned+KMS S3 per layer, least-privilege IAM, optional MSK |
| `infra/terraform/envs/{dev,qa,prod}.tfvars` | Per-environment sizing |
| `snowflake/ddl/001_setup.sql` / `002_tables.sql` | Idempotent role/warehouse/schema/table bootstrap |
| `.github/workflows/ci.yml` | Lint, typecheck, test matrix, wheel, Docker, terraform validate |
| `.github/workflows/deploy.yml` | Reusable: OIDC → terraform apply → bundle deploy → ECR → Snowflake DDL |
| `.github/workflows/cd-{dev,qa,prod}.yml` | Promotion triggers: main / rc tags / semver tags |
| `ci/jenkins/Jenkinsfile` | Equivalent declarative pipeline with prod `input` gate |

## Scale-out seam — `flink/`, `pinot/`
| File | Purpose |
|---|---|
| `flink/cdc_orders_to_iceberg.sql` | Flink SQL: native debezium-json → Iceberg v2 upsert table |
| `pinot/orders_schema.json` / `orders_realtime_table.json` | Realtime upsert table for sub-second serving |
| `docs/scaling_pinot_iceberg_flink.md` | When (and when not) to adopt each |

## Tests — `tests/`
| File | Purpose |
|---|---|
| `conftest.py` | Session-scoped Delta Spark fixture + isolated lakehouse root |
| `test_config.py` | Interpolation + overlay precedence |
| `test_expectations.py` | warn/drop/fail semantics incl. failure raising |
| `test_silver_transforms.py` | Dedup determinism, text cleansing, null safety |
| `test_cdc_merge.py` | Latest-per-key collapse; upsert + delete MERGE correctness |
| `test_landing_polars.py` | Landing schema contract (no Spark required) |

## Docs — `docs/`
`architecture.md` (mermaid + layer contracts) · `cdc.md` (envelope decision, watermark limits) · `deployment.md` (promotion matrix, secrets) · `scaling_pinot_iceberg_flink.md` (adoption criteria).
