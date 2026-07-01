"""Data quality checks used as gate tasks before a warehouse load.

Each check returns a list of human-readable violations (empty means it passed).
run_gate composes them and raises DataQualityError if anything failed, which
fails the gate task and, because load is downstream with the default
all_success rule, prevents the bad batch from ever being loaded.

Table and column names here are code-controlled constants, not user input, so
interpolating them into SQL is safe.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityError(Exception):
    """Raised when a batch fails one or more data quality checks."""


def check_row_count(
    hook: PostgresHook, table: str, load_date: date | str, min_rows: int = 1
) -> list[str]:
    count = hook.get_first(
        f"SELECT count(*) FROM {table} WHERE load_date = %(d)s",
        parameters={"d": load_date},
    )[0]
    if count < min_rows:
        return [f"row count {count} is below the minimum {min_rows}"]
    return []


def check_not_null(
    hook: PostgresHook, table: str, load_date: date | str, columns: Sequence[str]
) -> list[str]:
    violations: list[str] = []
    for column in columns:
        nulls = hook.get_first(
            f"SELECT count(*) FROM {table} WHERE load_date = %(d)s AND {column} IS NULL",
            parameters={"d": load_date},
        )[0]
        if nulls:
            violations.append(f"{nulls} null value(s) in column '{column}'")
    return violations


def check_non_negative(
    hook: PostgresHook, table: str, load_date: date | str, column: str
) -> list[str]:
    negatives = hook.get_first(
        f"SELECT count(*) FROM {table} WHERE load_date = %(d)s AND {column} < 0",
        parameters={"d": load_date},
    )[0]
    if negatives:
        return [f"{negatives} negative value(s) in column '{column}'"]
    return []


def run_gate(
    hook: PostgresHook,
    table: str,
    load_date: date | str,
    *,
    not_null_columns: Sequence[str],
    non_negative_column: str | None = None,
    min_rows: int = 1,
) -> None:
    """Run all checks and raise DataQualityError if any violation is found."""
    violations: list[str] = []
    violations += check_row_count(hook, table, load_date, min_rows=min_rows)
    violations += check_not_null(hook, table, load_date, not_null_columns)
    if non_negative_column is not None:
        violations += check_non_negative(hook, table, load_date, non_negative_column)

    if violations:
        raise DataQualityError(
            f"data quality gate failed for {table} on {load_date}: " + "; ".join(violations)
        )
