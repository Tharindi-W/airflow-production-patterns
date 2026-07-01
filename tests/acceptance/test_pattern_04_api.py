"""Acceptance test for Pattern 04: API Ingestion with Throttling.

Two levels:

  - Fast unit checks of the resilience primitives (no Postgres needed): the
    retry helper recovers from transient failures and gives up after exhausting
    retries; the mock API throttles then succeeds.
  - An end-to-end check (needs the full stack): the DAG survives injected 429s
    and 5xx and lands the expected number of rows.

    source scripts/env.sh
    pytest tests/acceptance/test_pattern_04_api.py -m acceptance -v
"""

from __future__ import annotations

import os
import subprocess

import pytest

pytestmark = pytest.mark.acceptance

DAG_ID = "api_ingestion_with_throttling"
LOGICAL_DATE = "2024-06-01"
EXPECTED_ROWS = 5 * 20  # pages * rows_per_page


def test_retry_recovers_from_transient_failures() -> None:
    from include.python_utils.backoff import retry_with_backoff

    calls = {"n": 0}

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("transient")
        return "ok"

    # sleep is a no-op so the test is instant.
    result = retry_with_backoff(flaky, retries=5, sleep=lambda _s: None)
    assert result == "ok"
    assert calls["n"] == 3


def test_retry_gives_up_after_exhausting() -> None:
    from include.python_utils.backoff import retry_with_backoff

    def always_fails() -> None:
        raise ValueError("permanent")

    with pytest.raises(ValueError):
        retry_with_backoff(always_fails, retries=2, sleep=lambda _s: None)


def test_mock_api_throttles_then_succeeds() -> None:
    from include.python_utils.mock_api import ApiError, MockThrottledApi

    api = MockThrottledApi(pages=1, rows_per_page=3, failures_before_success=2)
    # First two attempts raise, third returns rows.
    for _ in range(2):
        with pytest.raises(ApiError):
            api.get_page(0)
    rows = api.get_page(0)
    assert len(rows) == 3


def test_dag_survives_injected_throttling() -> None:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    result = subprocess.run(
        ["airflow", "dags", "test", DAG_ID, LOGICAL_DATE],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    assert (
        result.returncode == 0
    ), f"airflow dags test failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"

    hook = PostgresHook(postgres_conn_id="warehouse")
    count = hook.get_first(
        "SELECT count(*) FROM core.api_events WHERE load_date = %(load_date)s",
        parameters={"load_date": LOGICAL_DATE},
    )[0]
    assert int(count) == EXPECTED_ROWS, f"expected {EXPECTED_ROWS} rows after ingest, got {count}"
