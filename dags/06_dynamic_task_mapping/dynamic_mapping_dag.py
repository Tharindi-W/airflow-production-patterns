"""Pattern 06: Dynamic Task Mapping.

The number of work units is often unknown until runtime: today there are three
input files, tomorrow there are forty. Hard-coding a fixed set of tasks cannot
express that. Dynamic task mapping generates one mapped task instance per input
at run time with .expand(), so the DAG shape follows the data.

Drop a variable number of inputs, then run the DAG, and the mapped task count
matches the number of inputs. See the folder README.
"""

from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.python_utils.inputs import input_files

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}


@dag(
    dag_id="dynamic_task_mapping",
    description="Pattern 06: generate N mapped tasks at runtime from N input files.",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pattern-06", "dynamic-mapping"],
    doc_md=__doc__,
)
def dynamic_task_mapping():
    @task
    def create_tables() -> None:
        sql = (
            Path(__file__).resolve().parents[2]
            / "include"
            / "sql"
            / "06_dynamic_mapping"
            / "create_tables.sql"
        ).read_text(encoding="utf-8")
        PostgresHook(postgres_conn_id="warehouse").run(sql)

    @task
    def list_inputs(ds: str | None = None) -> list[str]:
        """Discover the inputs for this date at runtime. This drives the fan-out."""
        paths = [str(p) for p in input_files(ds)]
        print(f"list_inputs: found {len(paths)} inputs for {ds}")
        return paths

    @task
    def process_one(path: str) -> str:
        """Process a single input. One mapped instance runs per input file."""
        ds = get_current_context()["ds"]
        p = Path(path)
        value = int(p.read_text(encoding="utf-8").strip())
        PostgresHook(postgres_conn_id="warehouse").run(
            """
            INSERT INTO core.file_ingest (load_date, input_name, value)
            VALUES (%(load_date)s, %(input_name)s, %(value)s)
            ON CONFLICT (load_date, input_name) DO UPDATE
                SET value = EXCLUDED.value, loaded_at = now()
            """,
            parameters={"load_date": ds, "input_name": p.name, "value": value},
        )
        return p.name

    @task
    def summarize(processed: list[str], ds: str | None = None) -> dict:
        hook = PostgresHook(postgres_conn_id="warehouse")
        count = hook.get_first(
            "SELECT count(*) FROM core.file_ingest WHERE load_date = %(load_date)s",
            parameters={"load_date": ds},
        )[0]
        print(f"summarize: mapped {len(processed)} inputs, warehouse has {count} rows for {ds}")
        return {"load_date": str(ds), "mapped": len(processed), "rows": int(count)}

    tables = create_tables()
    listed = list_inputs()
    tables >> listed
    mapped = process_one.expand(path=listed)
    summarize(mapped)


dynamic_task_mapping()
