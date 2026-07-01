"""Pattern 09: Multi-System Orchestration.

Real pipelines are systems, not tasks. This one orchestrates a flow that spans
four systems, each behind a clear interface so the cloud pieces are mockable:

  API  -->  object store  -->  transform  -->  warehouse  -->  BI refresh

  extract_from_api      pull raw records from a (mock) API
  stage_to_object_store write the raw payload to a (mock) object store
  transform             read raw, compute revenue, write curated to the store
  load_to_warehouse     upsert the curated data into Postgres
  refresh_bi            trigger a (mock) BI dashboard refresh

Every hop that would be a cloud service in production is a labelled mock here,
so the whole chain runs locally with no credentials. See the folder README.
"""

from __future__ import annotations

import json
import random
from datetime import date
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.python_utils.mock_object_store import LocalObjectStore

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}

PRODUCTS = ("widget", "gadget", "gizmo")


def _generate_raw_orders(load_date: date, n: int = 40) -> list[dict]:
    """MOCK API extract: deterministic raw orders for a date."""
    rng = random.Random(int(load_date.strftime("%Y%m%d")))
    return [
        {
            "order_id": f"{load_date:%Y%m%d}-{i:04d}",
            "product": rng.choice(PRODUCTS),
            "qty": rng.randint(1, 20),
            "price": round(rng.uniform(5.0, 100.0), 2),
        }
        for i in range(n)
    ]


@dag(
    dag_id="multi_system_orchestration",
    description="Pattern 09: API to object store to transform to warehouse to BI, end to end with mocks.",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pattern-09", "orchestration", "multi-system"],
    doc_md=__doc__,
)
def multi_system_orchestration():
    @task
    def create_tables() -> None:
        sql = (
            Path(__file__).resolve().parents[2]
            / "include"
            / "sql"
            / "09_multi_system"
            / "create_tables.sql"
        ).read_text(encoding="utf-8")
        PostgresHook(postgres_conn_id="warehouse").run(sql)

    @task
    def extract_from_api(ds: str | None = None) -> str:
        """Pull raw records from the mock API and stage them to the object store."""
        load_date = pendulum.parse(ds).date()
        raw = _generate_raw_orders(load_date)
        store = LocalObjectStore()
        key = store.put(f"raw/{ds}.json", json.dumps(raw))
        print(f"extract_from_api: staged {len(raw)} raw records at {key}")
        return key

    @task
    def transform(raw_key: str, ds: str | None = None) -> str:
        """Read raw from the object store, compute revenue, write curated back."""
        store = LocalObjectStore()
        raw = json.loads(store.get(raw_key))
        curated = [
            {
                "order_id": r["order_id"],
                "load_date": ds,
                "product": r["product"],
                "revenue": round(r["qty"] * r["price"], 2),
            }
            for r in raw
        ]
        key = store.put(f"curated/{ds}.json", json.dumps(curated))
        print(f"transform: wrote {len(curated)} curated records at {key}")
        return key

    @task
    def load_to_warehouse(curated_key: str, ds: str | None = None) -> int:
        """Upsert the curated data from the object store into Postgres."""
        store = LocalObjectStore()
        curated = json.loads(store.get(curated_key))
        hook = PostgresHook(postgres_conn_id="warehouse")
        hook.insert_rows(
            table="core.curated_orders",
            rows=[(c["order_id"], c["load_date"], c["product"], c["revenue"]) for c in curated],
            target_fields=["order_id", "load_date", "product", "revenue"],
            replace=True,
            replace_index="order_id",
            commit_every=500,
        )
        print(f"load_to_warehouse: upserted {len(curated)} rows for {ds}")
        return len(curated)

    @task
    def refresh_bi(ds: str | None = None) -> None:
        """MOCK BI refresh: record that the dashboard was refreshed."""
        PostgresHook(postgres_conn_id="warehouse").run(
            """
            INSERT INTO core.bi_refreshes (load_date, dashboard)
            VALUES (%(d)s, %(dash)s)
            ON CONFLICT (load_date, dashboard) DO UPDATE SET refreshed_at = now()
            """,
            parameters={"d": ds, "dash": "sales_overview"},
        )
        print(f"refresh_bi: refreshed sales_overview dashboard for {ds} (mock)")

    raw_key = extract_from_api()
    curated_key = transform(raw_key)
    loaded = load_to_warehouse(curated_key)

    create_tables() >> raw_key
    loaded >> refresh_bi()


multi_system_orchestration()
