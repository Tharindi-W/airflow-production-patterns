"""Reusable warehouse load helpers for the idempotent ETL pattern.

These functions wrap the Postgres warehouse so that DAG task code stays thin
and the idempotency logic lives in one tested place. They are deliberately
plain: a PostgresHook plus parameterised SQL. The same helpers are used by the
DAG tasks and by the acceptance test, so the test exercises the real code path.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.python_utils.hashing import hash_rows

# Airflow connection id for the mock warehouse. Injected via the
# AIRFLOW_CONN_WAREHOUSE environment variable (see scripts/env.sh).
WAREHOUSE_CONN_ID = "warehouse"

# Business columns that define the logical content of a row. loaded_at is
# intentionally excluded: it is an operational timestamp, not business data,
# and it changes on every write.
BUSINESS_COLUMNS = ("transaction_id", "load_date", "account_id", "amount", "currency")

_SQL_DIR = Path(__file__).resolve().parents[2] / "include" / "sql" / "01_idempotent_etl"


def get_hook(conn_id: str = WAREHOUSE_CONN_ID) -> PostgresHook:
    """Return a PostgresHook for the warehouse connection."""
    return PostgresHook(postgres_conn_id=conn_id)


def _read_sql(filename: str) -> str:
    return (_SQL_DIR / filename).read_text(encoding="utf-8")


def ensure_tables(hook: PostgresHook) -> None:
    """Create the schema, target table, and staging table if absent."""
    hook.run(_read_sql("create_tables.sql"))


def stage_records(hook: PostgresHook, load_date: date, records: Sequence[dict]) -> int:
    """Replace the staging rows for one logical date with a fresh extract.

    Delete-then-insert scoped to load_date makes staging itself idempotent: a
    re-run for the same date rebuilds exactly that date's staging slice and
    leaves other dates untouched.
    """
    hook.run(
        "DELETE FROM core.stg_transactions WHERE load_date = %(load_date)s",
        parameters={"load_date": load_date},
    )
    rows = [
        (
            r["transaction_id"],
            r["load_date"],
            r["account_id"],
            r["amount"],
            r["currency"],
        )
        for r in records
    ]
    hook.insert_rows(
        table="core.stg_transactions",
        rows=rows,
        target_fields=list(BUSINESS_COLUMNS),
        commit_every=500,
    )
    return len(rows)


def upsert_from_staging(hook: PostgresHook, load_date: date) -> None:
    """Upsert one logical date's staged rows into the final table by primary key."""
    hook.run(_read_sql("upsert.sql"), parameters={"load_date": load_date})


def row_count(hook: PostgresHook, load_date: date) -> int:
    """Return the number of final-table rows for a logical date."""
    result = hook.get_first(
        "SELECT count(*) FROM core.transactions WHERE load_date = %(load_date)s",
        parameters={"load_date": load_date},
    )
    return int(result[0])


def fetch_business_rows(hook: PostgresHook, load_date: date) -> list[tuple]:
    """Return the business columns for a logical date, ordered by primary key."""
    sql = (
        "SELECT transaction_id, load_date, account_id, amount, currency "
        "FROM core.transactions WHERE load_date = %(load_date)s "
        "ORDER BY transaction_id"
    )
    return hook.get_records(sql, parameters={"load_date": load_date})


def content_hash(hook: PostgresHook, load_date: date) -> str:
    """Return a stable content hash of a logical date's business rows."""
    return hash_rows(fetch_business_rows(hook, load_date))
