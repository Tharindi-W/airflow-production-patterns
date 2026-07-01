"""Pattern 04: API Ingestion with Throttling.

External APIs throttle and fail. A pipeline that pulls from one must survive
429 Too Many Requests and 5xx errors without a human clearing tasks by hand.
This DAG pulls from a labelled mock API that deliberately injects transient
failures, and it absorbs them with three techniques:

  1. Client-side rate limiting: a minimum interval between calls, so we do not
     provoke throttling in the first place.
  2. Exponential backoff with jitter: each transient failure is retried after
     a growing, slightly randomised delay.
  3. Structured failure logging: every retry and give-up is logged with context.

The custom operator (plugins/custom_operators/resilient_api_operator.py) owns
the pagination, rate limiting, retry policy, and idempotent upsert.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.custom_operators.resilient_api_operator import ThrottledApiIngestOperator

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}

PAGES = 5
ROWS_PER_PAGE = 20


@dag(
    dag_id="api_ingestion_with_throttling",
    description="Pattern 04: resilient ingestion from a flaky, throttling (mock) API.",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pattern-04", "api", "retries", "throttling"],
    doc_md=__doc__,
)
def api_ingestion_with_throttling():
    @task
    def create_tables() -> None:
        hook = PostgresHook(postgres_conn_id="warehouse")
        from pathlib import Path

        sql = (
            Path(__file__).resolve().parents[2]
            / "include"
            / "sql"
            / "04_api_ingestion"
            / "create_tables.sql"
        ).read_text(encoding="utf-8")
        hook.run(sql)

    ingest = ThrottledApiIngestOperator(
        task_id="ingest",
        conn_id="warehouse",
        target_table="core.api_events",
        pages=PAGES,
        rows_per_page=ROWS_PER_PAGE,
        # Inject two transient failures per page so the backoff is exercised.
        failures_before_success=2,
    )

    @task
    def verify(ds: str | None = None) -> dict:
        hook = PostgresHook(postgres_conn_id="warehouse")
        count = hook.get_first(
            "SELECT count(*) FROM core.api_events WHERE load_date = %(load_date)s",
            parameters={"load_date": ds},
        )[0]
        print(f"verify: load_date={ds} rows={count}")
        return {"load_date": str(ds), "rows": int(count)}

    create_tables() >> ingest >> verify()


api_ingestion_with_throttling()
