"""MOCK upstream data source for the ETL patterns.

THIS IS A MOCK. It stands in for a real upstream extract: an API pull, a
database read, or a file drop. It is labelled as a mock so no one mistakes it
for a real integration.

The generator is deterministic: given the same logical date it always returns
the same rows. That determinism is exactly what makes the idempotency
acceptance test meaningful, because any change in the loaded data after a
re-run would then have to come from the load logic, not from the source.
"""

from __future__ import annotations

import random
from datetime import date

CURRENCIES = ("USD", "EUR", "GBP")
DEFAULT_ROW_COUNT = 100


def generate_transactions(load_date: date, n: int = DEFAULT_ROW_COUNT) -> list[dict]:
    """Return n deterministic transaction records for a logical date.

    The random number generator is seeded from the date, so the same date
    always yields byte-for-byte the same records. Transaction ids are scoped to
    the date, so different dates never collide on a primary key.
    """
    seed = int(load_date.strftime("%Y%m%d"))
    rng = random.Random(seed)

    records: list[dict] = []
    for i in range(n):
        records.append(
            {
                "transaction_id": f"{load_date:%Y%m%d}-{i:05d}",
                "load_date": load_date,
                "account_id": f"AC{rng.randint(1000, 9999)}",
                "amount": round(rng.uniform(1.0, 1000.0), 2),
                "currency": rng.choice(CURRENCIES),
            }
        )
    return records
