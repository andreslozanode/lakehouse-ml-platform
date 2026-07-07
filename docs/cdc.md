# CDC Design

Two complementary paths keep OLTP state synchronized with Silver:

## 1. Real-time: Debezium → Kafka → Structured Streaming

- Debezium (`pgoutput` plugin, logical replication) captures row-level changes from Postgres into Kafka topics `oltp.public.<table>`.
- The consumer reads the **full Debezium envelope** (`before`/`after`/`op`/`ts_ms`/`source`) instead of flattening with `ExtractNewRecordState`. Rationale: deletes remain first-class (`op = 'd'` with `before` payload), and source ordering metadata stays available for conflict resolution.
- `foreachBatch` collapses each micro-batch to the **latest event per primary key** (window on `ts_ms`) before a single Delta `MERGE` that upserts `c/u/r` and deletes `d`. This makes replays and at-least-once Kafka delivery idempotent.
- Checkpoints per table under `_checkpoints/cdc_<table>`; `--once` switches the trigger to `availableNow` for controlled backfills.

## 2. Batch: high-watermark JDBC sync

- Watermarks persist in a Delta control table (`_meta/watermarks`), one row per sync, so the job is restartable and auditable.
- Each run extracts `WHERE updated_at > :watermark` via JDBC, merges on primary key, then advances the watermark to `max(updated_at)` of the extracted set.
- Limitation (by design): pure watermark sync cannot see hard deletes — those are covered by the streaming path or by periodic reconciliation. Both paths converge on the same Silver tables through the same `merge_changes` primitive, so they can coexist safely.

## Downstream propagation

Silver CDC tables can enable Delta **Change Data Feed** so Gold consumers read only net changes instead of full scans — the hook is already isolated in `lakehouse/cdc/merge.py`.
