"""Pattern 02: Backfill-Safe Pipeline.

Historical reprocessing must not corrupt data. Two properties make this safe:

  1. catchup=False. The DAG does not silently stampede through every missed
     date the moment it is unpaused. Backfills are a deliberate, controlled
     action, not an accident.
  2. Partition-aware, date-scoped writes. Each run loads only its own logical
     date's partition (delete-then-insert keyed on load_date). Backfilling a
     date range therefore rebuilds each date independently, and no date can
     overwrite another.

See the folder README for how this differs from a naive full-table reload.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from include.python_utils import partitioned_load as pl
from include.python_utils.mock_source import generate_daily_sales

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}


@dag(
    dag_id="backfill_safe_pipeline",
    description="Pattern 02: partition-aware, backfill-safe loads keyed on the logical date.",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pattern-02", "backfill", "partitioning"],
    doc_md=__doc__,
)
def backfill_safe_pipeline():
    @task
    def create_tables() -> None:
        pl.ensure_tables(pl.get_hook())

    @task
    def load(ds: str | None = None) -> int:
        """Rebuild only this logical date's partition."""
        load_date = pendulum.parse(ds).date()
        rows = generate_daily_sales(load_date)
        written = pl.load_partition(pl.get_hook(), load_date, rows)
        print(f"load: wrote {written} rows for partition load_date={load_date}")
        return written

    @task
    def verify(ds: str | None = None) -> dict:
        load_date = pendulum.parse(ds).date()
        hook = pl.get_hook()
        count = pl.partition_row_count(hook, load_date)
        digest = pl.partition_hash(hook, load_date)
        print(f"verify: load_date={load_date} rows={count} partition_hash={digest}")
        return {"load_date": str(load_date), "rows": count, "partition_hash": digest}

    create_tables() >> load() >> verify()


backfill_safe_pipeline()
