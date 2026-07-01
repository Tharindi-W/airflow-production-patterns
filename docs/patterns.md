# Patterns (deeper writeups)

Each pattern has a focused folder README with four sections (why, failure modes, tradeoffs, production alternatives). This file is the index and the one-paragraph orientation for each. For the short version, see [pattern_catalog.md](pattern_catalog.md).

## 01. Idempotent ETL Pipeline

The flagship. A load scoped to one logical date, written with a primary key upsert (`INSERT ... ON CONFLICT ... DO UPDATE`), so running the same date twice produces no duplicates. Operational columns like `loaded_at` are excluded from the content hash, so equality checks compare business data only. Proven by running the DAG twice and asserting identical row counts and content hash. See [dags/01_idempotent_etl_pipeline/README.md](../dags/01_idempotent_etl_pipeline/README.md).

## 02. Backfill-Safe Pipeline

Partition-aware loads keyed on the logical date, with `catchup=False` so backfills are deliberate. Each run rebuilds only its own `load_date` partition via a scoped delete-then-insert, so backfilling a range rebuilds each date independently and no date overwrites another. See [dags/02_backfill_safe_pipeline/README.md](../dags/02_backfill_safe_pipeline/README.md).

## 03. Event-Driven Sensor Pattern

A custom reschedule-mode sensor waits for a per-date landing marker and proceeds only when it arrives, releasing its worker slot between pokes. Includes the poke vs reschedule vs deferrable comparison and a producer script that simulates the event. See [dags/03_event_driven_sensor_pattern/README.md](../dags/03_event_driven_sensor_pattern/README.md).

## 04. API Ingestion with Throttling

A custom operator pulls every page from a labelled mock API that injects 429 and 5xx, absorbing them with client-side rate limiting, exponential backoff with jitter, structured failure logging, and an idempotent upsert. Proven by surviving the injected throttling and landing the expected rows. See [dags/04_api_ingestion_with_throttling/README.md](../dags/04_api_ingestion_with_throttling/README.md).

## 05. Slow Upstream Handling

Per-stage retry boundaries (the fast source retries, the slow source fails fast), an `execution_timeout` to bound a hang, and `trigger_rule=all_done` so a timed-out optional source does not cascade. Proven by showing the fast source and report complete while the slow source is absent. See [dags/05_slow_upstream_handling/README.md](../dags/05_slow_upstream_handling/README.md).

## 06. Dynamic Task Mapping

One mapped task per runtime input via `.expand()`. An upstream task discovers the inputs present for the date, and the mapped task fans out to match, each instance idempotent by primary key. Proven by dropping a known number of inputs and confirming the mapped count matches. See [dags/06_dynamic_task_mapping/README.md](../dags/06_dynamic_task_mapping/README.md).

## 07. Retries and Failure Isolation

A partial-success workflow built from trigger rules: `all_done` on the aggregate so it proceeds on the survivors, and `one_failed` on a handler that fires exactly once when any branch fails, plus per-task retries. Proven by forcing one branch to fail and asserting the rest completes and the handler fires. See [dags/07_retries_and_failure_isolation/README.md](../dags/07_retries_and_failure_isolation/README.md).

## 08. Data Quality Gates

A batch is staged, run through a gate (row count, not-null, non-negative amount), and loaded only if the gate passes. The gate has `retries=0` because a quality violation is not transient. Proven by loading a good batch and blocking a bad one before it reaches the final table. See [dags/08_data_quality_gates/README.md](../dags/08_data_quality_gates/README.md).

## 09. Multi-System Orchestration

A flow spanning four systems (API, object store, transform, warehouse, BI refresh), each behind a clear interface with the cloud pieces mocked. Data is staged as raw then curated objects and upserted into the warehouse, with a BI refresh marker as the final hop. Proven by running the whole chain locally and asserting the data landed. See [dags/09_multi_system_orchestration/README.md](../dags/09_multi_system_orchestration/README.md).

## 10. Observability and Monitoring Hooks

Reusable `on_failure_callback` and `on_success_callback` (in `plugins/callbacks/notifications.py`) build a standardised alert payload and hand it to a mock notifier that logs it and records it to `core.alerts`. Any DAG can consume them via `default_args`. Proven by a failing task firing the callback and recording a well-formed alert. See [dags/10_production_monitoring_hooks/README.md](../dags/10_production_monitoring_hooks/README.md).
