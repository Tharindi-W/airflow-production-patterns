"""Acceptance test for Pattern 10: Observability and Monitoring Hooks.

Two levels:

  - A fast unit check of the payload builder (no Postgres): build_alert_payload
    produces a well-formed payload from a minimal context.
  - An end-to-end check (needs the full stack): a failing task fires the
    failure callback, which records a well-formed alert in core.alerts.

    source scripts/env.sh
    pytest tests/acceptance/test_pattern_10_observability.py -m acceptance -v
"""

from __future__ import annotations

import json
import types

import pytest

from tests.acceptance._runners import run_backfill

pytestmark = pytest.mark.acceptance

DAG_ID = "observability_hooks"
LOGICAL_DATE = "2024-12-15"
REQUIRED_KEYS = {"dag_id", "task_id", "run_id", "logical_date", "state", "channel"}


def test_build_alert_payload_is_well_formed() -> None:
    from plugins.callbacks.notifications import build_alert_payload

    context = {
        "task_instance": types.SimpleNamespace(dag_id="d", task_id="t", run_id="r", try_number=1),
        "dag": types.SimpleNamespace(dag_id="d"),
        "task": types.SimpleNamespace(task_id="t"),
        "run_id": "r",
        "logical_date": "2024-12-15",
        "exception": ValueError("boom"),
    }
    payload = build_alert_payload(context, state="failed")

    assert REQUIRED_KEYS.issubset(payload.keys())
    assert payload["dag_id"] == "d"
    assert payload["task_id"] == "t"
    assert payload["state"] == "failed"
    assert "boom" in payload["exception"]


def _alert_for(task_id: str, state: str) -> dict | None:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="warehouse")
    row = hook.get_first(
        "SELECT payload FROM core.alerts "
        "WHERE dag_id = %(dag)s AND task_id = %(task)s AND state = %(state)s "
        "ORDER BY alert_id DESC LIMIT 1",
        parameters={"dag": DAG_ID, "task": task_id, "state": state},
    )
    if row is None:
        return None
    payload = row[0]
    return payload if isinstance(payload, dict) else json.loads(payload)


def test_failure_fires_callback_with_well_formed_alert() -> None:
    run_backfill(DAG_ID, LOGICAL_DATE)

    failure_alert = _alert_for("failing_task", "failed")
    assert failure_alert is not None, "a failure alert should have been recorded"
    assert REQUIRED_KEYS.issubset(failure_alert.keys()), "alert payload is missing required keys"
    assert failure_alert["dag_id"] == DAG_ID
    assert failure_alert["task_id"] == "failing_task"
    assert failure_alert["exception"], "a failure alert should carry the exception text"

    # The success callback also fired for the healthy task.
    success_alert = _alert_for("healthy_task", "success")
    assert success_alert is not None, "a success alert should have been recorded"
