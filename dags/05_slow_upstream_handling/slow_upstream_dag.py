"""Pattern 05: Slow Upstream Handling.

Upstream systems are unreliable and sometimes just slow. A slow or hung source
must not stall the whole pipeline or cascade into downstream failures. This DAG
shows three defences:

  1. Per-stage retry boundaries. The fast, critical source retries; the slow,
     optional source is set to fail fast (retries=0) so it does not multiply a
     long wait by the retry count.
  2. A hard timeout. The slow source has an execution_timeout, so a hang is
     bounded: it fails at the timeout instead of running forever.
  3. Isolation via trigger rule. The report step runs with all_done, so it
     proceeds on whatever sources reported in. A slow source that timed out is
     simply absent, not fatal.

The slow source is a labelled simulation (a sleep longer than its timeout).
"""

from __future__ import annotations

import time

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=10),
}

# The slow source sleeps longer than its timeout, so it always times out.
SLOW_SLEEP_SECONDS = 8


def _note_status(load_date: str, source: str, status: str) -> None:
    PostgresHook(postgres_conn_id="warehouse").run(
        """
        INSERT INTO core.source_status (load_date, source, status)
        VALUES (%(load_date)s, %(source)s, %(status)s)
        ON CONFLICT (load_date, source) DO UPDATE
            SET status = EXCLUDED.status, noted_at = now()
        """,
        parameters={"load_date": load_date, "source": source, "status": status},
    )


@dag(
    dag_id="slow_upstream_handling",
    description="Pattern 05: bound a slow upstream with a timeout and isolate it from the critical path.",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pattern-05", "timeouts", "isolation"],
    doc_md=__doc__,
)
def slow_upstream_handling():
    @task
    def create_tables() -> None:
        from pathlib import Path

        sql = (
            Path(__file__).resolve().parents[2]
            / "include"
            / "sql"
            / "05_slow_upstream"
            / "create_tables.sql"
        ).read_text(encoding="utf-8")
        PostgresHook(postgres_conn_id="warehouse").run(sql)

    @task
    def fetch_fast(ds: str | None = None) -> None:
        """The reliable, critical source. Retries per default_args."""
        _note_status(ds, "fast", "ok")
        print(f"fetch_fast: recorded ok for {ds}")

    @task(
        retries=0,
        execution_timeout=pendulum.duration(seconds=2),
    )
    def fetch_slow(ds: str | None = None) -> None:
        """A slow/optional source. Fails fast on timeout, does not retry.

        It sleeps longer than its execution_timeout, so it is killed at the
        timeout and never records its status. That absence is the whole point:
        the report below still runs.
        """
        time.sleep(SLOW_SLEEP_SECONDS)
        _note_status(ds, "slow", "ok")  # never reached under the timeout

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def build_report(ds: str | None = None) -> dict:
        """Runs regardless of the slow source. Reports which sources arrived."""
        hook = PostgresHook(postgres_conn_id="warehouse")
        rows = hook.get_records(
            "SELECT source FROM core.source_status "
            "WHERE load_date = %(load_date)s AND status = 'ok' ORDER BY source",
            parameters={"load_date": ds},
        )
        available = [r[0] for r in rows]
        _note_status(ds, "report", "built")
        print(f"build_report: load_date={ds} available_sources={available}")
        return {"load_date": str(ds), "available_sources": available}

    tables = create_tables()
    fast = fetch_fast()
    slow = fetch_slow()
    report = build_report()

    tables >> [fast, slow] >> report


slow_upstream_handling()
