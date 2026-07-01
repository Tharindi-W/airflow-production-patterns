"""Acceptance test for Pattern 08: Data Quality Gates.

What it proves (the acceptance criterion): a good batch passes the gate and
loads, and a bad batch (with a null in a required column) is blocked before it
reaches the warehouse.

    source scripts/env.sh
    pytest tests/acceptance/test_pattern_08_quality.py -m acceptance -v
"""

from __future__ import annotations

import pytest

from tests.acceptance._runners import run_backfill, run_dag_test

pytestmark = pytest.mark.acceptance

DAG_ID = "data_quality_gates"
GOOD_DATE = "2024-10-01"
BAD_DATE = "2024-10-05"


def _orders_count(load_date: str) -> int:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="warehouse")
    return int(
        hook.get_first(
            "SELECT count(*) FROM core.orders WHERE load_date = %(d)s",
            parameters={"d": load_date},
        )[0]
    )


def _staging_count(load_date: str) -> int:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="warehouse")
    return int(
        hook.get_first(
            "SELECT count(*) FROM core.stg_orders WHERE load_date = %(d)s",
            parameters={"d": load_date},
        )[0]
    )


def test_good_batch_passes_gate_and_loads() -> None:
    result = run_dag_test(DAG_ID, GOOD_DATE)
    assert (
        result.returncode == 0
    ), f"good batch run failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert _orders_count(GOOD_DATE) > 0, "a good batch should load rows into the warehouse"


def test_bad_batch_is_blocked_before_load() -> None:
    # Inject a bad row and run through the scheduler.
    run_backfill(DAG_ID, BAD_DATE, extra_env={"PATTERNS_INJECT_BAD_BATCH": "1"})

    # The bad batch was staged (data was present) ...
    assert _staging_count(BAD_DATE) > 0, "the bad batch should have been staged"
    # ... but the gate blocked it, so nothing reached the final table.
    assert _orders_count(BAD_DATE) == 0, "the gate should have blocked the bad batch from loading"
