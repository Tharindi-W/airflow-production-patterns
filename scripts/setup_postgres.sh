#!/usr/bin/env bash
#
# One-time Postgres setup for the no-Docker (native) environment.
# This is the only script that needs sudo. It installs Postgres 16, starts it,
# and creates two databases:
#
#   airflow_meta  -> Airflow's own metadata database
#   warehouse     -> the mock data warehouse the patterns load into
#
# Run once:
#
#   ./scripts/setup_postgres.sh
#
# Safe to re-run: every step checks for existing state first (idempotent).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Load credentials from .env (or .env.example as a fallback).
if [ -f "${REPO_ROOT}/.env" ]; then
  set -a; . "${REPO_ROOT}/.env"; set +a
else
  set -a; . "${REPO_ROOT}/.env.example"; set +a
fi

AIRFLOW_DB="${AIRFLOW_DB:-airflow_meta}"
AIRFLOW_DB_USER="${AIRFLOW_DB_USER:-airflow}"
AIRFLOW_DB_PASSWORD="${AIRFLOW_DB_PASSWORD:-airflow}"
WAREHOUSE_DB="${WAREHOUSE_DB:-warehouse}"
WAREHOUSE_USER="${WAREHOUSE_USER:-warehouse}"
WAREHOUSE_PASSWORD="${WAREHOUSE_PASSWORD:-warehouse}"

echo "==> Installing Postgres (if not already present)"
if ! command -v psql >/dev/null 2>&1 || ! dpkg -l | grep -q "postgresql-16"; then
  sudo apt-get update
  sudo apt-get install -y postgresql postgresql-contrib
else
  echo "    Postgres already installed, skipping apt-get."
fi

echo "==> Starting the Postgres service"
# WSL does not run systemd by default, so use the service wrapper.
sudo service postgresql start
# Give the socket a moment to come up.
sleep 2

# Helper: run a SQL statement as the postgres superuser.
psql_super() {
  sudo -u postgres psql -v ON_ERROR_STOP=1 -tAc "$1"
}

create_role() {
  local role="$1" password="$2"
  if [ "$(psql_super "SELECT 1 FROM pg_roles WHERE rolname='${role}'")" = "1" ]; then
    echo "    Role '${role}' already exists."
  else
    echo "    Creating role '${role}'."
    psql_super "CREATE ROLE ${role} LOGIN PASSWORD '${password}'"
  fi
}

create_db() {
  local db="$1" owner="$2"
  if [ "$(psql_super "SELECT 1 FROM pg_database WHERE datname='${db}'")" = "1" ]; then
    echo "    Database '${db}' already exists."
  else
    echo "    Creating database '${db}' owned by '${owner}'."
    psql_super "CREATE DATABASE ${db} OWNER ${owner}"
  fi
}

echo "==> Creating roles"
create_role "${AIRFLOW_DB_USER}" "${AIRFLOW_DB_PASSWORD}"
create_role "${WAREHOUSE_USER}" "${WAREHOUSE_PASSWORD}"

echo "==> Creating databases"
create_db "${AIRFLOW_DB}" "${AIRFLOW_DB_USER}"
create_db "${WAREHOUSE_DB}" "${WAREHOUSE_USER}"

echo "==> Granting privileges"
psql_super "GRANT ALL PRIVILEGES ON DATABASE ${AIRFLOW_DB} TO ${AIRFLOW_DB_USER}"
psql_super "GRANT ALL PRIVILEGES ON DATABASE ${WAREHOUSE_DB} TO ${WAREHOUSE_USER}"
# Let the warehouse role create objects in the public schema (Postgres 15+ locks this down by default).
sudo -u postgres psql -v ON_ERROR_STOP=1 -d "${WAREHOUSE_DB}" -tAc "GRANT ALL ON SCHEMA public TO ${WAREHOUSE_USER}"

echo ""
echo "Postgres is ready."
echo "  metadata db : ${AIRFLOW_DB}   (user ${AIRFLOW_DB_USER})"
echo "  warehouse db: ${WAREHOUSE_DB} (user ${WAREHOUSE_USER})"
echo ""
echo "Next: ./scripts/setup_airflow.sh"
