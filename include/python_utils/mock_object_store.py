"""MOCK object store for the multi-system orchestration pattern.

THIS IS A MOCK. It stands in for a cloud object store (S3, GCS, Azure Blob) so
the pattern runs with no cloud account. It is labelled as a mock. Objects are
stored as files under a base directory, behind a small put/get/exists interface
that mirrors how you would call a real object store hook.

Swapping in a real store (for example the Amazon S3 provider hook) would change
only the body of these three methods, not the DAG that uses them.
"""

from __future__ import annotations

import os
from pathlib import Path


class LocalObjectStore:
    """A tiny file-backed object store with an S3-like key interface."""

    def __init__(self, base_dir: str | os.PathLike[str] | None = None) -> None:
        self.base = Path(
            base_dir
            or os.environ.get(
                "PATTERNS_OBJECT_STORE_DIR",
                str(Path.home() / ".airflow-patterns" / "objectstore"),
            )
        )

    def _path(self, key: str) -> Path:
        return self.base / key

    def put(self, key: str, data: str) -> str:
        """Write an object and return its key."""
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(data, encoding="utf-8")
        return key

    def get(self, key: str) -> str:
        """Read an object's contents by key."""
        return self._path(key).read_text(encoding="utf-8")

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()
