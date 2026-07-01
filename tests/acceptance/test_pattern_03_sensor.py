"""Acceptance test for Pattern 03: Event-Driven Sensor Pattern.

What it proves (the acceptance criterion): the sensor stays in a non-blocking
wait while the event is absent, then proceeds the moment the event arrives.

This test drives the custom sensor's poke() directly, which is the honest core
of the behaviour: poke() is False before the marker exists and True after. It
does not need Postgres, only Airflow installed and the repo env loaded.

    source scripts/env.sh
    pytest tests/acceptance/test_pattern_03_sensor.py -m acceptance -v
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.acceptance

DS = "2024-05-01"


def test_sensor_waits_then_proceeds() -> None:
    from include.python_utils.landing import clear_landing_file, drop_landing_file
    from plugins.custom_sensors.landing_sensor import LandingPartitionSensor

    # Bind the sensor to a concrete marker name (no templating needed here).
    sensor = LandingPartitionSensor(
        task_id="wait_for_landing_test",
        landing_subpath=f"{DS}.ready",
    )

    # Start from a clean slate: no marker present.
    clear_landing_file(DS)
    assert sensor.poke({}) is False, "sensor should wait while the event is absent"

    # The event arrives.
    marker = drop_landing_file(DS)
    try:
        assert marker.is_file(), "producer should have created the marker"
        assert sensor.poke({}) is True, "sensor should proceed once the event arrives"
    finally:
        clear_landing_file(DS)


def test_empty_marker_is_not_ready() -> None:
    """A zero-byte marker must not be treated as ready (partial write guard)."""
    from include.python_utils.landing import clear_landing_file, landing_path
    from plugins.custom_sensors.landing_sensor import LandingPartitionSensor

    sensor = LandingPartitionSensor(
        task_id="wait_for_landing_empty_test",
        landing_subpath=f"{DS}.ready",
    )
    clear_landing_file(DS)
    path = landing_path(DS)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")  # empty on purpose
    try:
        assert sensor.poke({}) is False, "empty marker should not count as ready"
    finally:
        clear_landing_file(DS)
