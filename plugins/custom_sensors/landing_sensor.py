"""A genuinely custom sensor: wait for a per-date landing marker file.

This is not a thin wrapper around FileSensor. It resolves a landing directory,
derives a per logical date marker filename from a template, requires the marker
to be non-empty (a zero byte file is treated as not ready), and logs a
structured line on every poke so the wait is observable in the task log.

It is meant to run in reschedule mode: between pokes it releases its worker
slot instead of holding it, which is the whole point of the pattern. See the
folder README for poke vs reschedule vs deferrable.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from airflow.sensors.base import BaseSensorOperator


def default_landing_dir() -> Path:
    """Directory where upstream drops per-date marker files.

    Overridable with PATTERNS_LANDING_DIR so tests and local runs can point it
    somewhere writable. Defaults to a folder under the Airflow home area.
    """
    return Path(
        os.environ.get(
            "PATTERNS_LANDING_DIR",
            str(Path.home() / ".airflow-patterns" / "landing"),
        )
    )


class LandingPartitionSensor(BaseSensorOperator):
    """Poke until a non-empty landing marker for the logical date exists."""

    template_fields = ("landing_subpath",)

    def __init__(
        self,
        *,
        landing_dir: str | os.PathLike[str] | None = None,
        landing_subpath: str = "{{ ds }}.ready",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.landing_dir = Path(landing_dir) if landing_dir else default_landing_dir()
        self.landing_subpath = landing_subpath

    def poke(self, context: Any) -> bool:
        target = self.landing_dir / self.landing_subpath
        ready = target.is_file() and target.stat().st_size > 0
        self.log.info(
            "LandingPartitionSensor poke: path=%s exists=%s ready=%s",
            target,
            target.exists(),
            ready,
        )
        return ready
