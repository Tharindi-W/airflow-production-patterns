# Pattern 05: Slow Upstream Handling

Upstream systems are unreliable, and the failure mode that catches people out is not a clean error but a slow one: a source that hangs, or takes ten times as long as usual. Without defences, one slow source stalls the whole pipeline and a worker slot sits blocked. This pattern bounds the slowness and isolates it.

```
  create_tables --> [ fetch_fast (retries)      ] --> build_report (all_done)
                    [ fetch_slow (timeout, no retry) ]
```

- DAG id: `slow_upstream_handling`
- Techniques: per-stage retry boundaries, `execution_timeout`, `trigger_rule=all_done`

## Why this pattern exists

A pipeline usually pulls from several sources. They do not all have the same reliability, and they should not all be treated the same way:

- The fast, critical source is worth retrying, because a transient blip should not fail the run.
- The slow, optional source should fail fast and get out of the way, because retrying a thing that hangs just multiplies the hang by the retry count.

Three settings encode that judgement:

1. Per-stage retry boundaries. Retries are set per task, not globally. The fast source inherits the default retries; the slow source is pinned to `retries=0`.
2. `execution_timeout`. The slow source is given a hard time budget. If it exceeds it, Airflow kills the task instead of letting it run forever. The hang becomes a bounded, visible failure.
3. Isolation with `trigger_rule=all_done`. The report step runs once its upstreams are done, whether they succeeded or failed. A timed-out source is simply absent from the results rather than fatal to them.

The acceptance test asserts exactly this: after a run, the fast source and the report are present, and the slow source (which timed out) recorded nothing.

## Failure modes (what breaks and when)

- Hung upstream. Without a timeout, the task waits forever and holds a slot. With `execution_timeout`, it fails at the budget. Here the slow source sleeps longer than its 2 second timeout and is always killed.
- Retry amplification. Retrying a slow task turns a 30 second hang into a multi-minute one. `retries=0` on the slow stage avoids that.
- Cascade. With the default `all_success` trigger rule, a failed upstream skips everything downstream, so one optional source failing would block the report. `all_done` breaks the cascade.
- Silent partial data. Because the report records which sources arrived, a downstream consumer can see that the slow source was missing rather than assuming completeness.

## Tradeoffs (why not the naive linear DAG)

A naive linear DAG treats every source as required and every failure as fatal, so its reliability is that of its worst source. Isolating sources costs a little more structure (per-task settings, a join with a trigger rule) and a decision about which sources are truly required. In return the pipeline degrades gracefully: it delivers what it can and clearly marks what it could not.

The tradeoff to manage is defining "critical" correctly. If you mark a genuinely required source as optional, you will happily publish incomplete data. The report's record of available sources is what keeps that honest.

## Production alternatives (what a large org reaches for)

- Deferrable operators and the triggerer so a long wait does not hold a worker slot at all.
- SLA misses and alerting so a source that is late but not yet failed still raises a signal.
- Circuit breakers and bulkheads at the service layer (for example in the API client or a service mesh) so a struggling dependency is shed automatically.
- Separate DAGs per source joined by Datasets, so a slow source cannot even share a run with a fast one.

This pattern uses plain per-task timeouts and trigger rules because they run locally and make the idea concrete, but the principle (bound the wait, isolate the blast radius, degrade gracefully) is the same at any scale.

## Run it

```bash
source scripts/env.sh

airflow dags test slow_upstream_handling 2024-07-01

pytest tests/acceptance/test_pattern_05_slow_upstream.py -m acceptance -v
```
