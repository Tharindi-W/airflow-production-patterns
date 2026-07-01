# Pattern 01: Idempotent ETL Pipeline (flagship)

Running the same logical date twice must not create duplicate rows in the warehouse. This is the most fundamental reliability property an ETL pipeline can have, and it is the one most often missing from pipelines written by people who learned Airflow from a tutorial.

```mermaid
flowchart LR
    A["🧱 create_tables"] --> B["📥 extract → staging"] --> C["🔀 load · upsert by PK"] --> D["🔎 verify · count + hash"]
```

- DAG id: `idempotent_etl_pipeline`
- Target: `core.transactions` in the `warehouse` database, primary key `transaction_id`
- Source: a labelled mock, deterministic per logical date (`include/python_utils/mock_source.py`)

## Why this pattern exists

In production, the same load runs more than once far more often than people expect:

- A task fails halfway and Airflow retries it.
- An operator clears a task or a whole DAG run to recover from an incident.
- A backfill re-runs a date that already loaded.
- Two schedulers or an at-least-once upstream deliver the same batch twice.

If the load appends blindly, every one of those events doubles data. Downstream metrics inflate, reconciliation breaks, and someone spends a weekend writing dedupe queries. An idempotent load makes a re-run a safe no-op, which turns recovery from a careful manual operation into "just run it again".

This pattern achieves idempotency two ways at once:

1. Execution date scoping. Every task operates on exactly the run's logical date (`ds`). Staging is rebuilt for that date with delete-then-insert, so a run only ever owns its own slice.
2. Primary key upsert. The final write is `INSERT ... ON CONFLICT (transaction_id) DO UPDATE`. A second run finds the same primary keys and updates them in place, so the row count does not grow and the business columns stay identical.

The `loaded_at` timestamp is refreshed on every write but is deliberately excluded from the content hash, because it is operational metadata, not business data.

## Failure modes (what breaks and when)

- Retry after a partial load. The extract task is interrupted after staging some rows. On retry, `stage_records` deletes that date's staging slice and rebuilds it, so the partial state is discarded before the upsert runs. No duplicates.
- Manual re-run / clear. An operator clears the DAG run for a date that already loaded. The upsert updates existing rows in place. Row count and content hash are unchanged. This is exactly what the acceptance test verifies.
- Non-deterministic source. If the source returned different data on each call (for example, `now()` baked into a field), the content hash would change on re-run even though the load logic is correct. We avoid this by using a deterministic mock keyed on the logical date. In production you would scope the extract to the date's data interval for the same reason.
- Primary key collision across dates. Transaction ids are namespaced by date (`YYYYMMDD-NNNNN`), so two different dates can never collide on the primary key and overwrite each other.

## Tradeoffs (why not the naive linear DAG)

A naive `extract >> INSERT` pipeline is simpler to write and is wrong the first time it is retried. The cost of idempotency here is:

- A staging table and an extra delete-then-insert step, which is a little more SQL and one more write.
- A primary key and an upsert, which is slightly slower than a blind append because Postgres must check for conflicts.

In exchange you get loads that are safe to retry, safe to backfill, and safe to clear, with no dedupe logic anywhere downstream. For any pipeline that feeds reporting or reconciliation, that trade is almost always worth it.

The main alternative shape is delete-by-partition then insert (truncate the date partition, then bulk insert). That is also idempotent and can be faster for large batches, but it briefly empties the partition, so a concurrent reader can see missing rows. The upsert approach keeps the table continuously correct, which is why it is the default here.

## Production alternatives (what a large org reaches for)

- Warehouse native MERGE. Snowflake, BigQuery, and Redshift all have `MERGE`, which expresses the same upsert in one statement against a columnar store sized for the volume.
- dbt with incremental models and a unique key, which generates the merge and manages the staging and target relations for you.
- A lakehouse table format (Delta Lake, Apache Iceberg, Apache Hudi) with `MERGE INTO`, giving idempotent upserts plus time travel on object storage.
- Exactly-once delivery further upstream (for example, Kafka with idempotent producers and transactional sinks) so duplicates are prevented before they reach the warehouse at all.

This pattern uses plain Postgres and `ON CONFLICT` because it runs locally with no cloud account, but the design (scope by partition, upsert by key, exclude operational columns from equality) is exactly what those managed tools implement under the hood.

## Run it

```bash
source scripts/env.sh

# Run the DAG once for a fixed logical date
airflow dags test idempotent_etl_pipeline 2024-01-01

# Prove idempotency: run the acceptance test (runs the DAG twice, checks
# row count and content hash are identical)
pytest tests/acceptance -m acceptance -v
```

Or open the UI at http://localhost:8080, unpause `idempotent_etl_pipeline`, and trigger it.
