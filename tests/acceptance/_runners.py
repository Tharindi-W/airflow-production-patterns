"""Shared helpers for running DAGs from acceptance tests.

Two ways to execute a DAG run, chosen by what the test needs:

  - run_dag_test: `airflow dags test`, a fast in-process run. Good when every
    task is expected to succeed. It aborts the run on the first unhandled task
    exception, so it cannot demonstrate downstream trigger-rule isolation.
  - run_backfill: `airflow dags backfill` via the LocalExecutor, which behaves
    like the scheduler. It continues past a failed task and runs downstream
    all_done / trigger-rule tasks, so it is the right choice for isolation and
    failure-handling patterns.
"""

from __future__ import annotations

import os
import subprocess


def run_dag_test(dag_id: str, logical_date: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["airflow", "dags", "test", dag_id, logical_date],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )


def run_backfill(dag_id: str, logical_date: str) -> subprocess.CompletedProcess:
    """Run one logical date through the scheduler-like backfill path.

    Returns the completed process. The return code may be non-zero when a task
    fails on purpose (that is the point of isolation patterns), so callers
    should assert on warehouse state rather than the exit code.
    """
    return subprocess.run(
        [
            "airflow",
            "dags",
            "backfill",
            "-s",
            logical_date,
            "-e",
            logical_date,
            "--reset-dagruns",
            "-y",
            dag_id,
        ],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
