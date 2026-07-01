"""Pattern 07: Retries and Failure Isolation.

One task failing should degrade the pipeline gracefully, not abort everything.
This DAG loads three regions in parallel. Two succeed, one is a controlled
failure. Trigger rules turn that into a partial-success workflow:

  - aggregate runs with trigger_rule=all_done, so it proceeds on whatever
    regions succeeded rather than being skipped because one failed.
  - handle_failure runs with trigger_rule=one_failed, so a failure anywhere in
    the region branches fires a failure handler exactly once.

The failing region also has its own retry budget, showing per-task retries
before the failure is finally accepted and isolated.
"""

from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}


def _note_region(load_date: str, region: str, status: str) -> None:
    PostgresHook(postgres_conn_id="warehouse").run(
        """
        INSERT INTO core.region_load (load_date, region, status)
        VALUES (%(load_date)s, %(region)s, %(status)s)
        ON CONFLICT (load_date, region) DO UPDATE
            SET status = EXCLUDED.status, noted_at = now()
        """,
        parameters={"load_date": load_date, "region": region, "status": status},
    )


def _note_event(load_date: str, event: str, detail: str) -> None:
    PostgresHook(postgres_conn_id="warehouse").run(
        """
        INSERT INTO core.run_events (load_date, event, detail)
        VALUES (%(load_date)s, %(event)s, %(detail)s)
        ON CONFLICT (load_date, event) DO UPDATE
            SET detail = EXCLUDED.detail, noted_at = now()
        """,
        parameters={"load_date": load_date, "event": event, "detail": detail},
    )


@dag(
    dag_id="retries_and_failure_isolation",
    description="Pattern 07: partial-success workflow using trigger rules and per-task retries.",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pattern-07", "trigger-rules", "failure-isolation"],
    doc_md=__doc__,
)
def retries_and_failure_isolation():
    @task
    def create_tables() -> None:
        sql = (
            Path(__file__).resolve().parents[2]
            / "include"
            / "sql"
            / "07_failure_isolation"
            / "create_tables.sql"
        ).read_text(encoding="utf-8")
        PostgresHook(postgres_conn_id="warehouse").run(sql)

    @task
    def fetch_region_a(ds: str | None = None) -> None:
        _note_region(ds, "A", "ok")

    @task
    def fetch_region_b(ds: str | None = None) -> None:
        _note_region(ds, "B", "ok")

    @task(retries=1, retry_delay=pendulum.duration(seconds=2))
    def fetch_region_c(ds: str | None = None) -> None:
        """A controlled failure. Retries once, then fails, and is isolated."""
        raise ValueError("region C upstream is unavailable (controlled failure)")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def aggregate(ds: str | None = None) -> dict:
        """Runs regardless of region C. Aggregates the regions that succeeded."""
        hook = PostgresHook(postgres_conn_id="warehouse")
        rows = hook.get_records(
            "SELECT region FROM core.region_load "
            "WHERE load_date = %(load_date)s AND status = 'ok' ORDER BY region",
            parameters={"load_date": ds},
        )
        regions = [r[0] for r in rows]
        _note_event(ds, "aggregate", f"regions={','.join(regions)}")
        print(f"aggregate: load_date={ds} succeeded_regions={regions}")
        return {"load_date": str(ds), "regions": regions}

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def handle_failure(ds: str | None = None) -> None:
        """Fires when any region failed. Records that the failure was handled."""
        _note_event(ds, "failure_handled", "at least one region failed")
        print(f"handle_failure: recorded failure handling for {ds}")

    tables = create_tables()
    a = fetch_region_a()
    b = fetch_region_b()
    c = fetch_region_c()
    tables >> [a, b, c]
    [a, b, c] >> aggregate()
    [a, b, c] >> handle_failure()


retries_and_failure_isolation()
