"""Partition-aware warehouse helpers for the backfill-safe pattern.

The unit of work is a logical date's partition. Loading a partition is a
delete-then-insert scoped to that date, so a run (scheduled or backfilled)
rebuilds only its own partition and never touches another date's rows. That is
what makes a backfill of a date range safe: the partitions are independent.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.python_utils.hashing import hash_rows
from include.python_utils.idempotent_load import WAREHOUSE_CONN_ID

_SQL_DIR = Path(__file__).resolve().parents[2] / "include" / "sql" / "02_backfill_safe"

_COLUMNS = ("load_date", "region", "txn_count", "total_amount")


def get_hook(conn_id: str = WAREHOUSE_CONN_ID) -> PostgresHook:
    return PostgresHook(postgres_conn_id=conn_id)


def ensure_tables(hook: PostgresHook) -> None:
    hook.run((_SQL_DIR / "create_tables.sql").read_text(encoding="utf-8"))


def load_partition(hook: PostgresHook, load_date: date, rows: Sequence[dict]) -> int:
    """Replace one date's partition with a fresh extract.

    Delete-then-insert scoped to load_date. Running this for date A leaves the
    partitions for every other date untouched, which is the whole point of the
    pattern.
    """
    hook.run(
        "DELETE FROM core.daily_sales WHERE load_date = %(load_date)s",
        parameters={"load_date": load_date},
    )
    values = [(r["load_date"], r["region"], r["txn_count"], r["total_amount"]) for r in rows]
    hook.insert_rows(
        table="core.daily_sales",
        rows=values,
        target_fields=list(_COLUMNS),
        commit_every=500,
    )
    return len(values)


def partition_row_count(hook: PostgresHook, load_date: date) -> int:
    result = hook.get_first(
        "SELECT count(*) FROM core.daily_sales WHERE load_date = %(load_date)s",
        parameters={"load_date": load_date},
    )
    return int(result[0])


def partition_hash(hook: PostgresHook, load_date: date) -> str:
    """Stable content hash of one date's partition (business columns only)."""
    rows = hook.get_records(
        "SELECT load_date, region, txn_count, total_amount "
        "FROM core.daily_sales WHERE load_date = %(load_date)s "
        "ORDER BY region",
        parameters={"load_date": load_date},
    )
    return hash_rows(rows)
