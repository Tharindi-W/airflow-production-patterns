#!/usr/bin/env bash
#
# Starts Airflow for local use. Uses "airflow standalone", which runs the
# scheduler, the triggerer, and the webserver together. Because env.sh sets the
# executor to LocalExecutor and the metadata DB to Postgres, standalone runs the
# real local stack (not the SQLite/SequentialExecutor default).
#
#   ./scripts/start_airflow.sh
#
# Then open http://localhost:8080 and log in with the admin credentials from
# .env (default admin / admin). Stop with Ctrl+C.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

# Make sure Postgres is up (WSL does not autostart it).
sudo service postgresql start >/dev/null 2>&1 || true

echo "Starting Airflow (standalone) on http://localhost:8080"
echo "Executor: ${AIRFLOW__CORE__EXECUTOR}   Metadata DB: Postgres"
echo "Press Ctrl+C to stop."
exec airflow standalone
