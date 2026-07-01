#!/usr/bin/env bash
#
# Single source of truth for the Airflow runtime environment.
# Source this file (do not execute it) before running any airflow command:
#
#   source scripts/env.sh
#
# It loads .env (your local values), then exports the AIRFLOW__* variables that
# point Airflow at the local Postgres metadata DB, the LocalExecutor, the repo's
# dags folder, and the mock warehouse connection.

# Resolve the repo root from this script's location, regardless of where it is
# sourced from.
_ENV_SH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export REPO_ROOT="$(cd "${_ENV_SH_DIR}/.." && pwd)"

# Load local values from .env if present, otherwise fall back to .env.example.
set -a
if [ -f "${REPO_ROOT}/.env" ]; then
  # shellcheck disable=SC1091
  . "${REPO_ROOT}/.env"
else
  echo "scripts/env.sh: no .env found, using defaults from .env.example" >&2
  # shellcheck disable=SC1091
  . "${REPO_ROOT}/.env.example"
fi
set +a

# Apply defaults for anything .env did not set.
export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export AIRFLOW_DB="${AIRFLOW_DB:-airflow_meta}"
export AIRFLOW_DB_USER="${AIRFLOW_DB_USER:-airflow}"
export AIRFLOW_DB_PASSWORD="${AIRFLOW_DB_PASSWORD:-airflow}"
export WAREHOUSE_DB="${WAREHOUSE_DB:-warehouse}"
export WAREHOUSE_USER="${WAREHOUSE_USER:-warehouse}"
export WAREHOUSE_PASSWORD="${WAREHOUSE_PASSWORD:-warehouse}"
export AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/.airflow-patterns}"
export AIRFLOW_VENV="${AIRFLOW_VENV:-$HOME/.venvs/airflow-patterns}"

# Core Airflow configuration, expressed as environment variables so the repo
# does not need to ship a generated airflow.cfg.
export AIRFLOW__CORE__EXECUTOR="LocalExecutor"
export AIRFLOW__CORE__DAGS_FOLDER="${REPO_ROOT}/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION="True"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASSWORD}@${PGHOST}:${PGPORT}/${AIRFLOW_DB}"

# Connection to the mock warehouse, injected as an Airflow connection via the
# AIRFLOW_CONN_<CONN_ID> convention. Patterns reference conn_id "warehouse".
export AIRFLOW_CONN_WAREHOUSE="postgresql://${WAREHOUSE_USER}:${WAREHOUSE_PASSWORD}@${PGHOST}:${PGPORT}/${WAREHOUSE_DB}"

# Make the repo's shared code importable from DAGs:
#   from include.python_utils...  and  from plugins.custom_operators...
export PYTHONPATH="${REPO_ROOT}:${PYTHONPATH:-}"

# Activate the project virtualenv if it exists.
if [ -f "${AIRFLOW_VENV}/bin/activate" ]; then
  # shellcheck disable=SC1091
  . "${AIRFLOW_VENV}/bin/activate"
fi
