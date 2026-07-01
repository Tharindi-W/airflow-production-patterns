# Failure Modes

A cross-pattern table of failure scenarios and how this repo handles each. Each pattern's folder README has a fuller "Failure modes" section.

| Scenario | Pattern(s) | How it is handled |
|----------|-----------|-------------------|
| Same load runs twice (retry, manual re-run, catchup) | 01 | Primary key upsert plus execution-date scoping, so the second run is a no-op on row counts and content hash. |
| Partial write interrupted mid load | 01, 02 | The load for a logical date is delete-then-insert within the date partition, so a re-run cleanly reestablishes the correct state. |
| Accidental catchup stampede on unpause | 02 | `catchup=False`, so backfills are a deliberate command, not an automatic flood. |
| Reprocessing corrupts other dates | 02 | Writes are scoped to one `load_date` partition, so a backfill of one date cannot touch another. |
| Waiting on an upstream file wastes a worker slot | 03 | Reschedule-mode sensor releases its slot between pokes instead of holding it. |
| Partial or premature event | 03 | A zero-byte marker is treated as not ready, so a half-written signal is not consumed. |
| External API returns 429 or 5xx | 04 | Exponential backoff with jitter retries transient failures; client-side rate limiting avoids provoking them. |
| Duplicate delivery on retry | 04, 06, 09 | Idempotent upsert by primary key, so a retried pull or mapped task never duplicates rows. |
| Upstream source hangs | 05 | `execution_timeout` bounds the wait and fails the task at the budget instead of blocking forever. |
| A slow or optional source fails | 05, 07 | Downstream runs with `trigger_rule=all_done`, so a failed source is absent rather than fatal. |
| Retry amplification on a slow task | 05 | The slow stage is set to `retries=0` so a hang is not multiplied by the retry count. |
| Unknown number of work units at author time | 06 | Dynamic task mapping fans out one task per runtime input via `.expand()`. |
| One unit of a fan-out fails | 06, 07 | Each unit is its own task instance with its own retry, so one failure is isolated and the rest complete. |
| A failure passes unnoticed | 07, 10 | An `all_done` failure detector (07) and failure callbacks (10) guarantee a signal when something fails. |
| Bad data (nulls, empty batch, bad values) reaches the warehouse | 08 | A quality gate staged before load fails the run on violation, blocking the load. |
| A quality violation is retried pointlessly | 08 | The gate task is `retries=0`, since a data quality failure is not transient. |
| Tight coupling across systems | 09 | Each hop is behind an interface with an object store in the middle, so systems can be swapped or mocked independently. |
| Expensive upstream repeated on re-run | 09 | Intermediate raw and curated artifacts persist in the object store, so a re-run can resume from a known point. |
| Silent failure at 3am | 10 | `on_failure_callback` fires a standardised alert with dag id, task id, run id, and the exception. |
| Inconsistent ad hoc alerting | 10 | A single reusable payload builder and notifier, consumed by any DAG via `default_args`. |
