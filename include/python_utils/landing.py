"""Helpers for the event-driven sensor pattern's landing markers.

A landing marker is a small per-date file that signals "the data for this date
has arrived and is ready to process". Real systems use an object-store object,
a manifest file, or a message. Here it is a local file, which is enough to
demonstrate the wait-then-proceed behaviour end to end.
"""

from __future__ import annotations

import os
from pathlib import Path


def landing_dir() -> Path:
    return Path(
        os.environ.get(
            "PATTERNS_LANDING_DIR",
            str(Path.home() / ".airflow-patterns" / "landing"),
        )
    )


def landing_path(ds: str) -> Path:
    """Return the marker path for a logical date string (YYYY-MM-DD)."""
    return landing_dir() / f"{ds}.ready"


def drop_landing_file(ds: str, content: str = "ready") -> Path:
    """Simulate the upstream event by writing a non-empty marker for a date."""
    path = landing_path(ds)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def clear_landing_file(ds: str) -> None:
    """Remove a date's marker if present (used to reset between test runs)."""
    landing_path(ds).unlink(missing_ok=True)
