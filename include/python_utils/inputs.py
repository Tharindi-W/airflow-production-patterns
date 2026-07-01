"""Helpers for the dynamic task mapping pattern's input files.

The pattern fans out one task per input file discovered at runtime. These
helpers manage a per-date input directory: dropping a variable number of input
files (to simulate an upstream that delivers an unknown count) and listing them
so the DAG can map over them.
"""

from __future__ import annotations

import os
from pathlib import Path


def inputs_root() -> Path:
    return Path(
        os.environ.get(
            "PATTERNS_INPUTS_DIR",
            str(Path.home() / ".airflow-patterns" / "inputs"),
        )
    )


def inputs_dir(ds: str) -> Path:
    return inputs_root() / ds


def input_files(ds: str) -> list[Path]:
    """Return the sorted list of input files for a logical date."""
    directory = inputs_dir(ds)
    if not directory.is_dir():
        return []
    return sorted(p for p in directory.iterdir() if p.is_file())


def drop_input_files(ds: str, n: int) -> list[Path]:
    """Create n input files for a date, each holding a deterministic value."""
    directory = inputs_dir(ds)
    directory.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for i in range(n):
        path = directory / f"part-{i:03d}.txt"
        path.write_text(str(i * 10), encoding="utf-8")
        paths.append(path)
    return paths


def clear_inputs(ds: str) -> None:
    """Remove all input files for a date (used to reset between runs)."""
    directory = inputs_dir(ds)
    if directory.is_dir():
        for path in directory.iterdir():
            if path.is_file():
                path.unlink()
