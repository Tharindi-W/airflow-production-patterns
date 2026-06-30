#!/usr/bin/env bash
#
# Installs Airflow into a project virtualenv and initialises its metadata DB.
# No sudo required. Run after scripts/setup_postgres.sh:
#
#   ./scripts/setup_airflow.sh
#
# Safe to re-run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Pin the same Airflow line everywhere.
AIRFLOW_VERSION="2.10.5"
PY_VERSION="3.12"
CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VERSION}.txt"

# Load runtime env (defines AIRFLOW_HOME, AIRFLOW_VENV, the SQL conn, etc.).
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

echo "==> Creating virtualenv at ${AIRFLOW_VENV}"
if [ ! -d "${AIRFLOW_VENV}" ]; then
  python3 -m venv "${AIRFLOW_VENV}"
fi
# shellcheck disable=SC1091
source "${AIRFLOW_VENV}/bin/activate"

echo "==> Upgrading pip"
python -m pip install --upgrade pip

echo "==> Installing Airflow ${AIRFLOW_VERSION} and providers (with constraints)"
pip install -r "${REPO_ROOT}/requirements.txt" --constraint "${CONSTRAINTS}"

echo "==> Installing dev and test tooling"
pip install -r "${REPO_ROOT}/requirements-dev.txt"

echo "==> Preparing AIRFLOW_HOME at ${AIRFLOW_HOME}"
mkdir -p "${AIRFLOW_HOME}"

echo "==> Initialising the Airflow metadata database"
airflow db migrate

echo "==> Creating the web UI admin user (if absent)"
if ! airflow users list 2>/dev/null | grep -q "${AIRFLOW_ADMIN_USER:-admin}"; then
  airflow users create \
    --username "${AIRFLOW_ADMIN_USER:-admin}" \
    --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
    --firstname Local \
    --lastname Admin \
    --role Admin \
    --email admin@example.com
else
  echo "    Admin user already exists."
fi

echo ""
echo "Airflow is installed and the metadata DB is ready."
echo "  AIRFLOW_HOME : ${AIRFLOW_HOME}"
echo "  venv         : ${AIRFLOW_VENV}"
echo "  dags folder  : ${AIRFLOW__CORE__DAGS_FOLDER}"
echo ""
echo "Next: ./scripts/start_airflow.sh   then open http://localhost:8080"
