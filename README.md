# Airflow Production Patterns

A reference catalogue of production grade Apache Airflow orchestration patterns, the kind a senior data engineer designs for banks, telecoms, and logistics platforms. Each DAG is a design pattern, each folder is a real world scenario, and each README explains the tradeoffs the way a consultant would. The value is in the patterns and the reasoning, not in any single dataset.

This repo runs entirely locally with no Docker and no cloud credentials. The whole stack is Airflow plus Postgres, running natively inside WSL (or any Linux or macOS shell).

## Architecture (local stack)

```
  WSL Ubuntu (or any Linux / macOS)
  +-----------------------------------------------------------+
  |                                                           |
  |   Python venv (Airflow 2.10.5, LocalExecutor)             |
  |   +---------------------+      +----------------------+    |
  |   | scheduler           |      | webserver  :8080     |    |
  |   | triggerer           |      | (Airflow UI)         |    |
  |   +----------+----------+      +----------+-----------+    |
  |              |                            |                |
  |              v                            v                |
  |   +-------------------------------------------------+      |
  |   | Postgres 16 (localhost:5432)                    |      |
  |   |   - airflow_meta : Airflow metadata DB          |      |
  |   |   - warehouse    : mock data warehouse          |      |
  |   +-------------------------------------------------+      |
  |                                                           |
  +-----------------------------------------------------------+

  DAGs are read from this repo's dags/ folder. Shared code lives in
  include/ (python_utils, sql) and plugins/ (custom operators, sensors,
  callbacks). No data leaves your machine.
```

There is no Docker in this setup by design. Airflow and Postgres run as native processes. See [docs/architecture.md](docs/architecture.md) for detail.

## Quickstart (no Docker)

Prerequisites: a Linux shell with `python3` (3.11 or 3.12) and `sudo` access. On Windows this means WSL2 with Ubuntu. Run every command from the repo root inside that shell. If the repo is on the Windows drive, that is `/mnt/c/Users/<you>/Desktop/airflow-production-patterns`.

```bash
# 1. Copy the local environment file (local dev credentials, not secrets)
cp .env.example .env

# 2. Install and initialise Postgres 16 (the only step that uses sudo)
./scripts/setup_postgres.sh

# 3. Create the Airflow venv, install pinned Airflow, init the metadata DB
./scripts/setup_airflow.sh

# 4. Start Airflow (scheduler + triggerer + webserver)
./scripts/start_airflow.sh
```

Then open http://localhost:8080 and log in with the admin credentials from `.env` (default `admin` / `admin`). DAGs are paused by default; unpause the one you want and trigger it.

To run the DAG validation tests:

```bash
source scripts/env.sh
pytest tests/dag_validation -v
```

## Hard conventions

- Anything mocked is labelled as a mock.
- Every pattern is runnable locally end to end. No pattern is documentation only.
- Exact pinned versions in `requirements.txt`. No floating `latest`.

## The patterns

Status legend: done means implemented, tested, and documented. Planned means specified but not yet built.

| #  | Pattern | What it proves | Status |
|----|---------|----------------|--------|
| 01 | [Idempotent ETL](dags/01_idempotent_etl_pipeline/) | Running the same load twice produces no duplicates | done |
| 02 | [Backfill safe pipeline](dags/02_backfill_safe_pipeline/) | Historical reprocessing without partitions overwriting each other | done |
| 03 | [Event driven sensor](dags/03_event_driven_sensor_pattern/) | Wait efficiently in reschedule mode rather than poll blindly | done |
| 04 | [API ingestion with throttling](dags/04_api_ingestion_with_throttling/) | Survive 429 and 5xx with backoff and rate limiting | done |
| 05 | Slow upstream handling | A slow stage does not cascade into downstream failure | planned |
| 06 | Dynamic task mapping | N runtime inputs produce N mapped tasks | planned |
| 07 | Retries and failure isolation | Partial success workflows with trigger rules | planned |
| 08 | Data quality gates | Bad batches are blocked before they reach the warehouse | planned |
| 09 | Multi system orchestration | API to object store to transform to warehouse, end to end with mocks | planned |
| 10 | Observability and monitoring hooks | Failure and success callbacks fire well formed alerts | planned |

## Repo layout

```
dags/        one folder per pattern, each with its own focused README
plugins/     custom operators, custom sensors, reusable callbacks
include/     shared python_utils, parameterised sql, configs
scripts/     setup and run scripts (Postgres, Airflow, env)
tests/       DAG validation and pattern acceptance tests
docs/        architecture, pattern catalog, failure modes, deeper writeups
```

## Documentation

- [docs/pattern_catalog.md](docs/pattern_catalog.md): the mini handbook, one line per pattern.
- [docs/architecture.md](docs/architecture.md): the local stack and how services connect.
- [docs/patterns.md](docs/patterns.md): deeper writeups expanding each folder README.
- [docs/failure_modes.md](docs/failure_modes.md): cross pattern table of failure scenarios.
