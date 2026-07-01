"""Acceptance test for Pattern 02: Backfill-Safe Pipeline.

Integration test (needs the full local stack, see tests/acceptance/README or
the Pattern 01 acceptance test for how to run).

What it proves (the acceptance criterion): backfilling a date range loads each
date's partition independently, and re-running one date does not overwrite the
others.

    source scripts/env.sh
    pytest tests/acceptance/test_pattern_02_backfill.py -m acceptance -v
"""

from __future__ import annotations

import os
import subprocess
from datetime import date

import pytest

pytestmark = pytest.mark.acceptance

DAG_ID = "backfill_safe_pipeline"
DATES = ["2024-03-01", "2024-03-02", "2024-03-03"]
REGIONS_PER_DATE = 3


def _run_dag(logical_date: str) -> None:
    result = subprocess.run(
        ["airflow", "dags", "test", DAG_ID, logical_date],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    assert result.returncode == 0, (
        f"airflow dags test failed for {logical_date}:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


def test_backfill_partitions_are_independent() -> None:
    from include.python_utils import partitioned_load as pl

    hook = pl.get_hook()
    dates = [date.fromisoformat(d) for d in DATES]

    # Backfill the range, one date at a time.
    for d in DATES:
        _run_dag(d)

    # Each date owns its own partition, with the same number of regions.
    for d in dates:
        assert (
            pl.partition_row_count(hook, d) == REGIONS_PER_DATE
        ), f"partition {d} should have {REGIONS_PER_DATE} rows"

    # Capture a hash per partition before re-running the middle date.
    hashes_before = {d: pl.partition_hash(hook, d) for d in dates}

    # Re-run the middle date. This must not disturb the neighbours.
    _run_dag(DATES[1])
    hashes_after = {d: pl.partition_hash(hook, d) for d in dates}

    for d in dates:
        assert (
            pl.partition_row_count(hook, d) == REGIONS_PER_DATE
        ), f"partition {d} row count changed after re-running {DATES[1]}"
        assert hashes_after[d] == hashes_before[d], (
            f"partition {d} content changed after re-running {DATES[1]}: "
            "a backfill overwrote a neighbouring partition"
        )
