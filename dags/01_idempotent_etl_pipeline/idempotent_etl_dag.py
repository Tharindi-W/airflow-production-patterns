"""Pattern 01: Idempotent ETL Pipeline (flagship).

Running the same logical date twice must not create duplicate rows in the
warehouse. This DAG proves that with two mechanisms working together:

  1. Execution date scoping. Every task works on exactly one logical date
     (the run's ds), so a run only ever owns one day's slice of data.
  2. Primary key upsert. Rows are written with INSERT ... ON CONFLICT
     (transaction_id) DO UPDATE, so re-writing the same transaction updates it
     in place instead of duplicating it.

The source is a labelled mock (include/python_utils/mock_source.py) that is
deterministic per date, which is what makes the run-twice acceptance test
meaningful. See the folder README for the full writeup.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from include.python_utils import idempotent_load as il
from include.python_utils.mock_source import generate_transactions

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}


@dag(
    dag_id="idempotent_etl_pipeline",
    description="Pattern 01: idempotent ETL with primary key upsert into the mock warehouse.",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pattern-01", "idempotency", "etl"],
    doc_md=__doc__,
)
def idempotent_etl_pipeline():
    @task
    def create_tables() -> None:
        """Create the warehouse schema and tables if they do not exist."""
        il.ensure_tables(il.get_hook())

    @task
    def extract(ds: str | None = None) -> int:
        """Pull the deterministic mock extract for this logical date into staging."""
        load_date = pendulum.parse(ds).date()
        records = generate_transactions(load_date)
        staged = il.stage_records(il.get_hook(), load_date, records)
        print(f"extract: staged {staged} rows for load_date={load_date}")
        return staged

    @task
    def load(ds: str | None = None) -> None:
        """Upsert this logical date's staged rows into the final table by primary key."""
        load_date = pendulum.parse(ds).date()
        il.upsert_from_staging(il.get_hook(), load_date)
        print(f"load: upserted staged rows for load_date={load_date}")

    @task
    def verify(ds: str | None = None) -> dict:
        """Report the row count and content hash for this logical date."""
        load_date = pendulum.parse(ds).date()
        hook = il.get_hook()
        count = il.row_count(hook, load_date)
        digest = il.content_hash(hook, load_date)
        print(f"verify: load_date={load_date} rows={count} content_hash={digest}")
        return {"load_date": str(load_date), "rows": count, "content_hash": digest}

    create_tables() >> extract() >> load() >> verify()


idempotent_etl_pipeline()
