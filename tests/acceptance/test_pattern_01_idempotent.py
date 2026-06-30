"""Acceptance test for Pattern 01: Idempotent ETL.

This is an integration test. It needs the full local stack running:
Postgres (metadata DB and warehouse) plus Airflow installed in the venv, with
the environment from scripts/env.sh loaded. It is marked "acceptance" so the
lightweight DAG validation suite can run without it.

How to run:

    source scripts/env.sh
    pytest tests/acceptance -m acceptance -v

What it proves (the acceptance criterion from the build spec): running the DAG
twice for the same logical date produces identical row counts and an identical
content hash, so the load created no duplicates.
"""

from __future__ import annotations

import os
import subprocess
from datetime import date

import pytest

pytestmark = pytest.mark.acceptance

DAG_ID = "idempotent_etl_pipeline"
LOGICAL_DATE = "2024-01-01"
EXPECTED_ROWS = 100


def _run_dag_once() -> None:
    """Execute the whole DAG synchronously for the fixed logical date."""
    result = subprocess.run(
        ["airflow", "dags", "test", DAG_ID, LOGICAL_DATE],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    assert result.returncode == 0, (
        "airflow dags test failed:\n" f"stdout:\n{result.stdout}\n" f"stderr:\n{result.stderr}"
    )


def test_rerun_produces_no_duplicates() -> None:
    from include.python_utils import idempotent_load as il

    load_date = date(2024, 1, 1)
    hook = il.get_hook()

    # First run establishes the data.
    _run_dag_once()
    count_after_first = il.row_count(hook, load_date)
    hash_after_first = il.content_hash(hook, load_date)

    assert (
        count_after_first == EXPECTED_ROWS
    ), f"expected {EXPECTED_ROWS} rows after first run, got {count_after_first}"

    # Second run for the same logical date must be a no-op on content.
    _run_dag_once()
    count_after_second = il.row_count(hook, load_date)
    hash_after_second = il.content_hash(hook, load_date)

    assert count_after_second == count_after_first, (
        "row count changed on re-run: the load is not idempotent "
        f"({count_after_first} then {count_after_second})"
    )
    assert (
        hash_after_second == hash_after_first
    ), "content hash changed on re-run: the loaded data is not stable"
