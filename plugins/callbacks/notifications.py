"""Reusable success and failure callbacks with a mock notifier.

These callbacks are the observability layer for the repo. They build a
well-formed alert payload from the Airflow task context and hand it to a
notifier. The notifier here is a labelled mock: it logs the payload it would
send and records it to core.alerts so the alert is observable and testable.
Swapping in a real Slack or email notifier would change only MockNotifier.send.

Any DAG can consume these by setting them in default_args, for example:

    default_args = {
        "on_failure_callback": on_failure_callback,
        "on_success_callback": on_success_callback,
    }
"""

from __future__ import annotations

import json
import logging
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

ALERTS_CONN_ID = "warehouse"
CHANNEL = "slack+email (mock)"


def build_alert_payload(context: dict[str, Any], state: str) -> dict[str, Any]:
    """Build a standardised alert payload from the Airflow task context.

    Reads defensively so it works both under a real task run and in a unit test
    that passes a minimal context.
    """
    ti = context.get("task_instance") or context.get("ti")
    dag = context.get("dag")
    task = context.get("task")
    exception = context.get("exception")

    dag_id = getattr(dag, "dag_id", None) or getattr(ti, "dag_id", None)
    task_id = getattr(ti, "task_id", None) or getattr(task, "task_id", None)
    run_id = context.get("run_id") or getattr(ti, "run_id", None)
    logical_date = context.get("logical_date") or context.get("ds")

    return {
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "logical_date": str(logical_date) if logical_date is not None else None,
        "state": state,
        "try_number": getattr(ti, "try_number", None),
        "exception": str(exception) if exception else None,
        "channel": CHANNEL,
    }


class MockNotifier:
    """MOCK notifier standing in for Slack or email.

    THIS IS A MOCK. It does not call any external service. It logs the payload
    it would send and records it to core.alerts so the alert is observable.
    """

    def __init__(self, conn_id: str = ALERTS_CONN_ID) -> None:
        self.conn_id = conn_id

    def send(self, payload: dict[str, Any]) -> None:
        log.info("ALERT (mock notifier) would send: %s", json.dumps(payload))
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        hook.run(
            """
            INSERT INTO core.alerts
                (dag_id, task_id, run_id, logical_date, state, try_number,
                 exception, channel, payload)
            VALUES
                (%(dag_id)s, %(task_id)s, %(run_id)s, %(logical_date)s, %(state)s,
                 %(try_number)s, %(exception)s, %(channel)s, %(payload)s::jsonb)
            """,
            parameters={
                "dag_id": payload.get("dag_id"),
                "task_id": payload.get("task_id"),
                "run_id": payload.get("run_id"),
                "logical_date": payload.get("logical_date"),
                "state": payload.get("state"),
                "try_number": payload.get("try_number"),
                "exception": payload.get("exception"),
                "channel": payload.get("channel"),
                "payload": json.dumps(payload),
            },
        )


def on_failure_callback(context: dict[str, Any]) -> None:
    """Attach to a task or DAG default_args to alert on failure."""
    payload = build_alert_payload(context, state="failed")
    log.error("task failed: %s.%s", payload["dag_id"], payload["task_id"])
    MockNotifier().send(payload)


def on_success_callback(context: dict[str, Any]) -> None:
    """Attach to a task or DAG default_args to alert on success."""
    payload = build_alert_payload(context, state="success")
    MockNotifier().send(payload)
