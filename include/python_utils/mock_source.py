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


REGIONS = ("NA", "EU", "APAC")


def generate_daily_sales(load_date: date) -> list[dict]:
    """Return one deterministic daily sales roll-up per region for a date.

    Used by the backfill-safe pattern. Like the transaction generator, this is
    deterministic per date: the same date always yields the same partition, so
    a re-run or a backfill of a date reproduces exactly that date's rows.
    """
    seed = int(load_date.strftime("%Y%m%d"))
    rng = random.Random(seed)

    records: list[dict] = []
    for region in REGIONS:
        txn_count = rng.randint(50, 500)
        avg_amount = rng.uniform(10.0, 250.0)
        records.append(
            {
                "load_date": load_date,
                "region": region,
                "txn_count": txn_count,
                "total_amount": round(txn_count * avg_amount, 2),
            }
        )
    return records


def generate_orders(load_date: date, n: int = 50, inject_bad: bool = False) -> list[dict]:
    """Return deterministic order rows for the data quality gate pattern.

    With inject_bad=True, one row is appended with a null customer_id, which is
    a deliberate data quality violation the gate must catch before load.
    """
    seed = int(load_date.strftime("%Y%m%d"))
    rng = random.Random(seed)

    records: list[dict] = []
    for i in range(n):
        records.append(
            {
                "order_id": f"{load_date:%Y%m%d}-O{i:04d}",
                "load_date": load_date,
                "customer_id": f"C{rng.randint(1000, 9999)}",
                "amount": round(rng.uniform(5.0, 500.0), 2),
            }
        )

    if inject_bad:
        # A row that violates the not-null gate on customer_id.
        records.append(
            {
                "order_id": f"{load_date:%Y%m%d}-OBAD",
                "load_date": load_date,
                "customer_id": None,
                "amount": round(rng.uniform(5.0, 500.0), 2),
            }
        )
    return records
