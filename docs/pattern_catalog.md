# Pattern Catalog

A one line statement of the production problem each pattern solves. This is the mini handbook. Deeper writeups live in [patterns.md](patterns.md) and in each pattern's folder README.

| #  | Pattern | Production problem it solves |
|----|---------|------------------------------|
| 01 | Idempotent ETL | A retried or re-run load must not create duplicate rows in the warehouse. |
| 02 | Backfill safe pipeline | Reprocessing history must touch only its own date partition, never another. |
| 03 | Event driven sensor | Waiting for an upstream file should not burn a worker slot the whole time. |
| 04 | API ingestion with throttling | A flaky, rate limited API must not take the pipeline down with it. |
| 05 | Slow upstream handling | A slow or failing upstream stage must not cascade into everything downstream. |
| 06 | Dynamic task mapping | The number of work units is unknown until runtime and must be fanned out. |
| 07 | Retries and failure isolation | One failed branch should degrade the run gracefully, not abort everything. |
| 08 | Data quality gates | Bad data must be stopped before it ever reaches the warehouse. |
| 09 | Multi system orchestration | A flow that spans several systems must be coordinated behind clear interfaces. |
| 10 | Observability and monitoring hooks | An orchestration with no visibility into failures is operationally blind. |

Status: all ten patterns are implemented, tested, and documented. Each passes the DAG validation suite and runs end to end locally with an acceptance test that proves its criterion.
