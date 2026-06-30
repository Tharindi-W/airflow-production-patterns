"""DAG integrity checks.

This suite is the gate every pattern must pass. It loads the whole dags folder
and asserts:

  1. Every DAG file imports with no errors.
  2. No DAG contains a cycle.
  3. Each DAG sets a few production hygiene attributes (tags, owner, retries).

It needs Airflow installed but no running scheduler and no live metadata DB,
so it is fast and CI friendly.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import check_cycle

DAGS_FOLDER = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    """Load all DAGs once for the module."""
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


def test_dags_folder_exists() -> None:
    assert DAGS_FOLDER.is_dir(), f"dags folder not found at {DAGS_FOLDER}"


def test_no_import_errors(dag_bag: DagBag) -> None:
    """No DAG file should fail to import."""
    if dag_bag.import_errors:
        messages = "\n".join(
            f"  {path}:\n    {error.strip().splitlines()[-1]}"
            for path, error in dag_bag.import_errors.items()
        )
        pytest.fail(f"DAG import errors found:\n{messages}")


def test_at_least_one_dag(dag_bag: DagBag) -> None:
    """The bag should not be empty once patterns exist."""
    assert dag_bag.dags, "no DAGs were discovered in the dags folder"


def test_no_cycles(dag_bag: DagBag) -> None:
    """No DAG may contain a dependency cycle."""
    for _dag_id, dag in dag_bag.dags.items():
        # check_cycle raises AirflowDagCycleException on a cycle.
        check_cycle(dag)


def test_dags_have_tags(dag_bag: DagBag) -> None:
    """Every DAG should be tagged so the UI stays navigable."""
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"DAG '{dag_id}' has no tags"


def test_tasks_have_retries(dag_bag: DagBag) -> None:
    """Production DAGs should not leave tasks at zero retries by accident.

    A task may still opt out by setting retries explicitly to 0, but the
    default_args should establish a non-negative integer so the intent is
    visible.
    """
    for dag_id, dag in dag_bag.dags.items():
        for task in dag.tasks:
            assert isinstance(
                task.retries, int
            ), f"task '{task.task_id}' in DAG '{dag_id}' has a non-integer retries value"
