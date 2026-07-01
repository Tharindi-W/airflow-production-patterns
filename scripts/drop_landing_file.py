#!/usr/bin/env python3
"""Simulate the upstream event for Pattern 03 by dropping a landing marker.

Usage (with the repo env loaded so imports resolve):

    source scripts/env.sh
    python scripts/drop_landing_file.py 2024-05-01

This writes a non-empty marker file that LandingPartitionSensor is waiting for,
which lets the event-driven DAG proceed. It is a labelled simulation of a real
upstream producer, not a real integration.
"""

from __future__ import annotations

import sys

from include.python_utils.landing import drop_landing_file


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: python scripts/drop_landing_file.py <YYYY-MM-DD>")
        return 2
    ds = argv[1]
    path = drop_landing_file(ds)
    print(f"dropped landing marker: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
