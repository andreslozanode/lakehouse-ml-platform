# Architecture

## Overview

The platform is a Medallion lakehouse (Bronze → Silver → Gold on Delta Lake) fed by two ingestion families — batch API pulls (Kaggle, Reddit) and CDC (Debezium over Postgres, in both streaming and batch flavors) — with an ML/AI layer that trains classical and deep models over the Gold feature table, and a Snowflake delivery path for BI.

```mermaid
flowchart LR
    subgraph Sources
        K[Kaggle Datasets API]
        R[Reddit public JSON API]
        PG[(Postgres OLTP)]
    end

    subgraph Ingestion
        LC[Polars normalizers<br/>landing zone]
        DBZ[Debezium<br/>Kafka Connect]
        KFK[(Kafka KRaft)]
    end

    subgraph Lakehouse [Delta Lakehouse - Databricks / local]
        B[Bronze<br/>append-only, lineage]
        S[Silver<br/>dedup, DQ gates, MERGE]
        G[Gold<br/>aggregates + feature table]
    end

    subgraph ML [ML / AI]
        F[Feature splits]
        M1[KNN / RandomForest<br/>scikit-learn]
        M2[DistilBERT<br/>PyTorch + CUDA AMP]
        M3[BiLSTM<br/>TensorFlow]
        REG[MLflow Registry<br/>champion alias]
        API[FastAPI serving]
    end

    SF[(Snowflake GOLD)]
    BI[BI / dashboards]

    K --> LC --> B
    R --> LC
    PG --> DBZ --> KFK -->|Structured Streaming<br/>full envelope MERGE| S
    PG -->|JDBC watermark<br/>hourly batch| S
    B --> S --> G
    G --> F --> M1 & M2 & M3 --> REG --> API
    G -->|write_pandas + MERGE| SF --> BI
```

## Layer contracts

| Layer | Guarantees | Write pattern |
|---|---|---|
| Landing | Raw vendor formats + normalized typed Parquet with `_MANIFEST.json` per batch | Immutable files |
| Bronze | Raw-but-typed, append-only, lineage columns (`_batch_id`, `_ingest_ts`, `_source`), replayable | `replaceWhere` on `_batch_id` (idempotent) |
| Silver | Deduplicated on natural keys, cleansed text, DQ gates (warn/drop/fail), CDC targets | Delta `MERGE` |
| Gold | Business aggregates, serving-ready joins, ML feature table | Overwrite (rebuildable from Silver) |

## Orchestration

Airflow owns the *schedules*; the package CLI owns the *logic*. Every DAG task shells into `python -m lakehouse.cli <stage>`, which means the identical code path runs locally, in Airflow, and as Databricks wheel tasks — no logic drift between environments.

## Configuration & environments

`conf/base.yaml` + `conf/{dev,qa,prod}.yaml` overlays with `${VAR:default}` interpolation. `LAKEHOUSE_ENV` selects the overlay; `LAKEHOUSE_ROOT` moves the entire lakehouse between local disk and S3 without code changes. Secrets resolve env → Databricks secret scope → AWS Secrets Manager.

## Why Polars *and* Spark

Landing normalization is single-node file wrangling where Polars' lazy streaming engine is faster and cheaper than spinning Spark; everything from Bronze onward is Spark/Delta because it needs MERGE, time travel, schema evolution and cluster scale. pandas appears only at well-defined edges (Snowflake `write_pandas`, sklearn interop).
