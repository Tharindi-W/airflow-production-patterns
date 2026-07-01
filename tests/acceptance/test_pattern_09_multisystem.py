"""Acceptance test for Pattern 09: Multi-System Orchestration.

What it proves (the acceptance criterion): the full chain (API to object store
to transform to warehouse to BI refresh) runs locally end to end with mocks,
and the data lands in the warehouse schema.

    source scripts/env.sh
    pytest tests/acceptance/test_pattern_09_multisystem.py -m acceptance -v
"""

from __future__ import annotations

import pytest

from tests.acceptance._runners import run_dag_test

pytestmark = pytest.mark.acceptance

DAG_ID = "multi_system_orchestration"
LOGICAL_DATE = "2024-11-01"
EXPECTED_ROWS = 40


def _curated_count(load_date: str) -> int:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="warehouse")
    return int(
        hook.get_first(
            "SELECT count(*) FROM core.curated_orders WHERE load_date = %(d)s",
            parameters={"d": load_date},
        )[0]
    )


def _bi_refreshed(load_date: str) -> bool:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="warehouse")
    count = hook.get_first(
        "SELECT count(*) FROM core.bi_refreshes "
        "WHERE load_date = %(d)s AND dashboard = 'sales_overview'",
        parameters={"d": load_date},
    )[0]
    return int(count) == 1


def test_full_chain_lands_data_and_refreshes_bi() -> None:
    result = run_dag_test(DAG_ID, LOGICAL_DATE)
    assert (
        result.returncode == 0
    ), f"multi-system run failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"

    # Data made it all the way through the chain into the warehouse.
    assert (
        _curated_count(LOGICAL_DATE) == EXPECTED_ROWS
    ), f"expected {EXPECTED_ROWS} curated rows, got {_curated_count(LOGICAL_DATE)}"
    # The final hop (mock BI refresh) fired.
    assert _bi_refreshed(LOGICAL_DATE), "the BI refresh marker should be present"
