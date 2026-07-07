# Scaling Path: Flink + Iceberg + Pinot

Short answer: yes, the trio is viable — but each solves a *different* bottleneck, and they should be adopted in this order as those bottlenecks actually appear.

## Apache Flink — adopt first, when CDC becomes the core workload

Spark Structured Streaming (current implementation) is micro-batch: practical latency is seconds to tens of seconds, and Debezium envelope handling is manual. Flink treats Debezium changelogs as a **native retract stream** (`format = 'debezium-json'`), giving true event-at-a-time processing, event-time semantics, and much richer stateful operators (temporal joins against changing dimensions, CEP). Adopt when: sub-second freshness matters, you need stream-stream/temporal joins over CDC state, or Spark checkpoint latency becomes the SLA blocker. `flink/cdc_orders_to_iceberg.sql` is the working seam — it consumes the *same* Kafka topics produced today, so adoption is additive, not a rewrite.

## Apache Iceberg — adopt when multi-engine access matters

Delta and Iceberg overlap heavily (ACID, time travel, schema evolution). Iceberg earns its place when the same tables must be written/read by Flink, Trino, Snowflake (native Iceberg tables) and Spark **without vendor coupling**, and its hidden partitioning + partition evolution removes a whole class of repartition migrations. With Databricks UC now reading Iceberg via UniForm, a pragmatic pattern is: keep Delta for the Databricks-centric medallion, and land the Flink CDC path in Iceberg v2 (upsert-enabled) as in the provided SQL. Don't run two formats for the same table long-term — pick per pipeline.

## Apache Pinot — adopt last, for user-facing analytics only

Pinot is not a lakehouse layer; it's a serving engine for **sub-second OLAP at high QPS** (dashboards embedded in products, real-time leaderboards, operational monitoring with thousands of concurrent queries). It ingests directly from Kafka with FULL upsert mode (see `pinot/orders_realtime_table.json`), so the Gold "orders enriched" stream can be queryable milliseconds after the OLTP commit. If your consumers are analysts on Snowflake/Databricks SQL, you do not need Pinot; if you're exposing metrics inside an application, it's the right tool and cheaper than hammering a warehouse.

## Decision matrix

| Signal | Add |
|---|---|
| CDC latency SLO < 5s, temporal joins, complex event logic | Flink |
| Multiple engines (Flink/Trino/Snowflake/Spark) on shared tables | Iceberg |
| User-facing dashboards, >100 QPS, p99 < 1s | Pinot |
| None of the above yet | Keep Spark + Delta (current design) — simplest to operate |

## Target end-state topology

```
Postgres → Debezium → Kafka ─→ Flink SQL ─→ Iceberg (silver, upsert v2) ─→ Spark/Trino/Snowflake
                          └──→ Pinot realtime table (gold serving, upsert) → product dashboards
```
