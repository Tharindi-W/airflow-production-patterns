"""Test configuration for DAG validation.

Sets lightweight Airflow environment defaults before Airflow is imported, so
the validation suite can run without a configured AIRFLOW_HOME or a live
metadata database. These defaults are only applied if the caller has not
already set them (for example via scripts/env.sh in local runs or via CI).
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _default_env(key: str, value: str) -> None:
    os.environ.setdefault(key, value)


# Keep example DAGs out of the bag and avoid touching a real metadata DB.
_default_env("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
_default_env("AIRFLOW__CORE__DAGS_FOLDER", str(REPO_ROOT / "dags"))
# A throwaway AIRFLOW_HOME so importing Airflow does not write into the repo.
_default_env("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-validation-"))
# Use an in-memory SQLite metadata DB. DagBag does not query the DB for the
# checks below, but Airflow still wants a valid connection string at import.
_default_env("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite://")
_default_env("AIRFLOW__CORE__EXECUTOR", "SequentialExecutor")

# Make the repo's shared code importable from DAG modules during validation.
import sys  # noqa: E402

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
