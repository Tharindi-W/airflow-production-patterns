"""Deterministic content hashing for warehouse rows.

Used by the idempotency patterns to prove that re-running a load produces
identical data. The hash is computed over the business columns only, in a
stable order, so two runs that load the same logical data produce the same
digest regardless of operational columns like load timestamps.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Sequence


def hash_rows(rows: Iterable[Sequence[object]]) -> str:
    """Return a stable SHA-256 hex digest for an ordered iterable of rows.

    Each row is a sequence of values. Values are rendered with str() and joined
    with a separator that cannot appear inside the rendered values causing a
    collision in practice. None is rendered as an empty string. The caller is
    responsible for passing rows in a deterministic order (for example, sorted
    by primary key).
    """
    digest = hashlib.sha256()
    for row in rows:
        rendered = "|".join("" if value is None else str(value) for value in row)
        digest.update(rendered.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()
