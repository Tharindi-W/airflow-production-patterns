"""Acceptance test for Pattern 06: Dynamic Task Mapping.

What it proves (the acceptance criterion): dropping a variable number of inputs
produces a matching number of mapped task instances at run time, confirmed by
one warehouse row per input.

    source scripts/env.sh
    pytest tests/acceptance/test_pattern_06_mapping.py -m acceptance -v
"""

from __future__ import annotations

import pytest

from tests.acceptance._runners import run_dag_test

pytestmark = pytest.mark.acceptance

DAG_ID = "dynamic_task_mapping"


def _rows_for(load_date: str) -> int:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="warehouse")
    return int(
        hook.get_first(
            "SELECT count(*) FROM core.file_ingest WHERE load_date = %(load_date)s",
            parameters={"load_date": load_date},
        )[0]
    )


@pytest.mark.parametrize(
    ("logical_date", "n_inputs"),
    [("2024-08-01", 4), ("2024-08-02", 2)],
)
def test_mapped_count_matches_inputs(logical_date: str, n_inputs: int) -> None:
    from include.python_utils.inputs import clear_inputs, drop_input_files

    # Deliver a specific number of inputs for this date.
    clear_inputs(logical_date)
    drop_input_files(logical_date, n_inputs)

    result = run_dag_test(DAG_ID, logical_date)
    assert (
        result.returncode == 0
    ), f"dag run failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"

    # One row per input means one mapped task ran per input.
    assert (
        _rows_for(logical_date) == n_inputs
    ), f"expected {n_inputs} rows for {n_inputs} inputs, got {_rows_for(logical_date)}"
