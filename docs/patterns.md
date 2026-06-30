# Patterns (deeper writeups)

This file expands on each pattern's folder README. It grows as patterns land. For the short version, see [pattern_catalog.md](pattern_catalog.md).

## 01. Idempotent ETL Pipeline

The flagship pattern. See [dags/01_idempotent_etl_pipeline/README.md](../dags/01_idempotent_etl_pipeline/README.md) for the full writeup. In short:

- The load is scoped to a single logical (execution) date, so a run only ever owns one day's slice of data.
- Rows are written with a primary key based upsert (`INSERT ... ON CONFLICT ... DO UPDATE`), so writing the same row twice updates it in place instead of duplicating it.
- Transformations are deterministic: the same input always produces the same output, which is what makes a content hash a meaningful equality check.
- The acceptance test runs the DAG twice for the same logical date and asserts that the warehouse row count and a content hash are identical after the second run.

## 02 through 10

Planned. Each will get a section here once implemented, expanding on the four section folder README (why, failure modes, tradeoffs, production alternatives).
