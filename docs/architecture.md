# Architecture

This repo runs Airflow and Postgres natively, with no Docker and no cloud services. The goal is a setup anyone can clone and run on a laptop in a few minutes, while staying close to how a real Airflow deployment is shaped.

## Components

| Component | What it is | Where it runs |
|-----------|------------|---------------|
| Airflow scheduler | Schedules and queues task instances | Python venv process |
| Airflow triggerer | Runs deferrable tasks (used by later patterns) | Python venv process |
| Airflow webserver | The UI on port 8080 | Python venv process |
| Postgres 16 | Two databases (see below) | Native service in WSL |
| Executor | LocalExecutor (real parallel task execution) | In the scheduler process |

We deliberately use the LocalExecutor with a Postgres metadata DB rather than the SequentialExecutor with SQLite. That gives genuine parallel task execution, which several patterns (dynamic task mapping, failure isolation) need in order to demonstrate anything interesting.

## The two databases

A single Postgres 16 server hosts two separate databases:

- `airflow_meta`: Airflow's own metadata (DAG runs, task instances, connections). Owned by the `airflow` role.
- `warehouse`: the mock data warehouse that the patterns load into. Owned by the `warehouse` role. This stands in for a Snowflake or BigQuery or Redshift target in a real system. It is labelled a mock so no one mistakes it for a production warehouse.

Keeping them as separate databases (not just separate schemas) mirrors production, where the orchestrator's state and the analytical warehouse are different systems entirely.

## How the repo connects to the runtime

The repo lives on disk (on Windows, under `/mnt/c/...` as seen from WSL). The Airflow runtime is configured entirely through environment variables set by `scripts/env.sh`, so the repo does not ship a generated `airflow.cfg`:

- `AIRFLOW__CORE__DAGS_FOLDER` points at this repo's `dags/` folder.
- `AIRFLOW__CORE__EXECUTOR` is `LocalExecutor`.
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` points at the `airflow_meta` database.
- `AIRFLOW_CONN_WAREHOUSE` injects an Airflow connection named `warehouse` pointing at the `warehouse` database, using the `AIRFLOW_CONN_<ID>` convention. Patterns reference `conn_id="warehouse"`.
- `PYTHONPATH` includes the repo root, so DAGs can import `include.python_utils` and `plugins.custom_operators`.

The Airflow home directory (logs, runtime files) is kept in the WSL home directory, off the slow Windows mount.

## Why no Docker

Docker Compose is the common way to bring up an Airflow stack, and it is a fine choice. This repo intentionally avoids it so that:

- It runs with zero container tooling, on any machine with Python and Postgres.
- The moving parts are visible. You can see the scheduler, the webserver, and the database as ordinary processes.

The tradeoff is that first time setup installs Postgres on the host (one scripted step) instead of pulling an image. The setup scripts are idempotent and safe to re-run.

## Setup and run scripts

| Script | Needs sudo | What it does |
|--------|------------|--------------|
| `scripts/setup_postgres.sh` | yes (once) | Installs Postgres 16, creates the two databases and roles |
| `scripts/setup_airflow.sh` | no | Creates the venv, installs pinned Airflow, initialises the metadata DB, creates the admin user |
| `scripts/env.sh` | no | Sourced to load the runtime environment |
| `scripts/start_airflow.sh` | no | Starts Airflow in standalone mode |
