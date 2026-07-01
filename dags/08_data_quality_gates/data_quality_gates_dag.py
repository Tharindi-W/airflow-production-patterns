"""Pattern 08: Data Quality Gates.

Stop bad data before it reaches the warehouse. This DAG stages a batch, runs it
through a quality gate (row count, null checks, non-negative amounts), and only
loads it if the gate passes. Because the load task is downstream of the gate
with the default all_success rule, a failed gate blocks the load automatically.

Set PATTERNS_INJECT_BAD_BATCH=1 to have the source append a row with a null
customer_id, which the gate catches and blocks. See the folder README.
"""

from __future__ import annotations

import os
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.python_utils.mock_source import generate_orders
from include.python_utils.quality import run_gate

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=1),
}

STG_TABLE = "core.stg_orders"
NOT_NULL_COLUMNS = ("order_id", "customer_id", "amount")


@dag(
    dag_id="data_quality_gates",
    description="Pattern 08: quality gate that blocks a bad batch before load.",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pattern-08", "data-quality"],
    doc_md=__doc__,
)
def data_quality_gates():
    @task
    def create_tables() -> None:
        sql = (
            Path(__file__).resolve().parents[2]
            / "include"
            / "sql"
            / "08_data_quality"
            / "create_tables.sql"
        ).read_text(encoding="utf-8")
        PostgresHook(postgres_conn_id="warehouse").run(sql)

    @task
    def extract(ds: str | None = None) -> int:
        """Stage the batch. May include a bad row if injection is enabled."""
        inject_bad = os.environ.get("PATTERNS_INJECT_BAD_BATCH") == "1"
        load_date = pendulum.parse(ds).date()
        rows = generate_orders(load_date, inject_bad=inject_bad)
        hook = PostgresHook(postgres_conn_id="warehouse")
        hook.run(
            "DELETE FROM core.stg_orders WHERE load_date = %(d)s",
            parameters={"d": load_date},
        )
        hook.insert_rows(
            table=STG_TABLE,
            rows=[(r["order_id"], r["load_date"], r["customer_id"], r["amount"]) for r in rows],
            target_fields=["order_id", "load_date", "customer_id", "amount"],
            commit_every=500,
        )
        print(f"extract: staged {len(rows)} rows for {ds} (inject_bad={inject_bad})")
        return len(rows)

    @task(retries=0)
    def quality_gate(ds: str | None = None) -> None:
        """Fail the run here if the staged batch violates any check.

        retries=0 on purpose: a data quality violation is not transient, so
        retrying would only waste time before the same failure recurs.
        """
        hook = PostgresHook(postgres_conn_id="warehouse")
        run_gate(
            hook,
            STG_TABLE,
            ds,
            not_null_columns=NOT_NULL_COLUMNS,
            non_negative_column="amount",
            min_rows=1,
        )
        print(f"quality_gate: batch for {ds} passed all checks")

    @task
    def load(ds: str | None = None) -> int:
        """Only reached if the gate passed. Upserts staging into the final table."""
        hook = PostgresHook(postgres_conn_id="warehouse")
        hook.run(
            """
            INSERT INTO core.orders (order_id, load_date, customer_id, amount)
            SELECT order_id, load_date, customer_id, amount
            FROM core.stg_orders
            WHERE load_date = %(d)s
            ON CONFLICT (order_id) DO UPDATE
                SET load_date = EXCLUDED.load_date,
                    customer_id = EXCLUDED.customer_id,
                    amount = EXCLUDED.amount,
                    loaded_at = now()
            """,
            parameters={"d": ds},
        )
        count = hook.get_first(
            "SELECT count(*) FROM core.orders WHERE load_date = %(d)s",
            parameters={"d": ds},
        )[0]
        print(f"load: warehouse has {count} orders for {ds}")
        return int(count)

    create_tables() >> extract() >> quality_gate() >> load()


data_quality_gates()
