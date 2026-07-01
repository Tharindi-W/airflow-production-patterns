"""Pattern 10: Observability and Monitoring Hooks.

An orchestration you cannot see into is operationally blind. This DAG wires the
reusable callbacks from plugins/callbacks/notifications.py so that:

  - a failing task fires on_failure_callback, which builds a well-formed alert
    payload and hands it to a mock notifier (logs it and records it to
    core.alerts),
  - a healthy task fires on_success_callback the same way.

The notifier is a labelled mock standing in for Slack or email. In production
you would swap it for a real notifier without touching the DAGs. See the folder
README.
"""

from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.callbacks.notifications import on_failure_callback, on_success_callback

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 0,
    "retry_delay": pendulum.duration(minutes=1),
}


@dag(
    dag_id="observability_hooks",
    description="Pattern 10: standardised alerting via reusable success and failure callbacks.",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pattern-10", "observability", "callbacks"],
    doc_md=__doc__,
)
def observability_hooks():
    @task
    def create_tables() -> None:
        sql = (
            Path(__file__).resolve().parents[2]
            / "include"
            / "sql"
            / "10_observability"
            / "create_tables.sql"
        ).read_text(encoding="utf-8")
        PostgresHook(postgres_conn_id="warehouse").run(sql)

    @task(on_success_callback=on_success_callback)
    def healthy_task() -> str:
        """A task that succeeds, to fire the success callback."""
        return "ok"

    @task(retries=0, on_failure_callback=on_failure_callback)
    def failing_task() -> None:
        """A controlled failure, to fire the failure callback and alert."""
        raise RuntimeError("simulated failure to exercise the alerting hook")

    create_tables() >> [healthy_task(), failing_task()]


observability_hooks()
