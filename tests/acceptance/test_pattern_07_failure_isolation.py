"""Acceptance test for Pattern 07: Retries and Failure Isolation.

What it proves (the acceptance criterion): forcing one branch to fail, the rest
completes per the documented trigger rules. The two healthy regions aggregate,
the failed region is isolated, and the failure handler fires.

    source scripts/env.sh
    pytest tests/acceptance/test_pattern_07_failure_isolation.py -m acceptance -v
"""

from __future__ import annotations

import pytest

from tests.acceptance._runners import run_backfill

pytestmark = pytest.mark.acceptance

DAG_ID = "retries_and_failure_isolation"
LOGICAL_DATE = "2024-09-15"


def _regions(load_date: str) -> dict[str, str]:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="warehouse")
    rows = hook.get_records(
        "SELECT region, status FROM core.region_load WHERE load_date = %(load_date)s",
        parameters={"load_date": load_date},
    )
    return {region: status for region, status in rows}


def _events(load_date: str) -> set[str]:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="warehouse")
    rows = hook.get_records(
        "SELECT event FROM core.run_events WHERE load_date = %(load_date)s",
        parameters={"load_date": load_date},
    )
    return {r[0] for r in rows}


def test_partial_success_and_failure_handling() -> None:
    run_backfill(DAG_ID, LOGICAL_DATE)

    regions = _regions(LOGICAL_DATE)
    events = _events(LOGICAL_DATE)

    # The healthy regions completed.
    assert regions.get("A") == "ok", "region A should have succeeded"
    assert regions.get("B") == "ok", "region B should have succeeded"
    # The failing region is isolated and recorded nothing.
    assert "C" not in regions, "region C should have failed and recorded nothing"

    # aggregate ran on the survivors (all_done), and the failure was handled
    # (one_failed).
    assert "aggregate" in events, "aggregate should run regardless (all_done)"
    assert "failure_handled" in events, "failure handler should fire (one_failed)"
