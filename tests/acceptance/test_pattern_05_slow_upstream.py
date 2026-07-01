"""Acceptance test for Pattern 05: Slow Upstream Handling.

What it proves (the acceptance criterion): a slow upstream that times out does
not cascade. The fast source and the report still complete, and the slow
source is simply absent from the results.

    source scripts/env.sh
    pytest tests/acceptance/test_pattern_05_slow_upstream.py -m acceptance -v
"""

from __future__ import annotations

import pytest

from tests.acceptance._runners import run_backfill

pytestmark = pytest.mark.acceptance

DAG_ID = "slow_upstream_handling"
LOGICAL_DATE = "2024-07-15"


def _sources_with_status(load_date: str) -> dict[str, str]:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="warehouse")
    rows = hook.get_records(
        "SELECT source, status FROM core.source_status WHERE load_date = %(load_date)s",
        parameters={"load_date": load_date},
    )
    return {source: status for source, status in rows}


def test_slow_upstream_is_isolated() -> None:
    # Run through the scheduler (backfill) so trigger-rule isolation applies.
    # The slow task times out (fails), but that is isolated from the rest.
    run_backfill(DAG_ID, LOGICAL_DATE)

    statuses = _sources_with_status(LOGICAL_DATE)

    # The fast source completed.
    assert statuses.get("fast") == "ok", "the fast/critical source should have completed"
    # The report ran despite the slow source timing out (all_done isolation).
    assert statuses.get("report") == "built", "the report should run regardless of the slow source"
    # The slow source timed out before recording, so it is absent.
    assert "slow" not in statuses, "the slow source should have timed out and recorded nothing"
