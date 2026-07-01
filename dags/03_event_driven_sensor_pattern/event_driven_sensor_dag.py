"""Pattern 03: Event-Driven Sensor Pattern.

Wait intelligently instead of polling blindly. The DAG blocks on a landing
marker for its logical date and proceeds only once that marker appears. The
sensor runs in reschedule mode, so while it waits it releases its worker slot
instead of holding one hostage.

Simulate the event from another shell while the DAG is waiting:

    source scripts/env.sh
    python scripts/drop_landing_file.py 2024-05-01

See the folder README for poke vs reschedule vs deferrable.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from include.python_utils.landing import landing_path
from plugins.custom_sensors.landing_sensor import LandingPartitionSensor

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=1),
}


@dag(
    dag_id="event_driven_sensor_pattern",
    description="Pattern 03: reschedule-mode landing sensor that waits for a per-date event.",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pattern-03", "sensors", "event-driven"],
    doc_md=__doc__,
)
def event_driven_sensor_pattern():
    wait_for_landing = LandingPartitionSensor(
        task_id="wait_for_landing",
        # reschedule frees the worker slot between pokes rather than holding it.
        mode="reschedule",
        poke_interval=10,
        timeout=60 * 10,
    )

    @task
    def process(ds: str | None = None) -> str:
        """Process the arrived data. Here it just reads the marker."""
        path = landing_path(ds)
        content = path.read_text(encoding="utf-8").strip()
        print(f"process: consumed landing marker {path} content={content!r}")
        return content

    wait_for_landing >> process()


event_driven_sensor_pattern()
