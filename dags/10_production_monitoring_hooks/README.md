# Pattern 10: Observability and Monitoring Hooks

Airflow is useless without visibility. A pipeline that fails silently at 3am and is discovered at 9am when a report is wrong has failed twice: once in the data, and once in the operations. This pattern standardises how the pipeline tells you something happened.

```
  create_tables --> healthy_task  (on_success_callback --> alert)
                --> failing_task  (on_failure_callback --> alert)
                                        |
                                        v
                                 mock notifier -> core.alerts
```

- DAG id: `observability_hooks`
- Reusable callbacks: `plugins/callbacks/notifications.py`
- Alerts sink: `core.alerts`

## Why this pattern exists

Every DAG needs the same operational plumbing: when a task fails, someone should be told, with enough context to act (which DAG, which task, which run, what went wrong). Writing that ad hoc in every DAG leads to inconsistent, half-finished alerting. The fix is a small reusable module that any DAG consumes.

Three pieces make it work:

- A payload builder that reads the Airflow task context and produces a standardised, well-formed alert: dag id, task id, run id, logical date, state, try number, and the exception. Standardised means every alert looks the same, so a human or a downstream system can parse it.
- Callbacks (`on_failure_callback`, `on_success_callback`) that Airflow invokes automatically when a task ends. Attach them in a DAG's `default_args` and every task is covered.
- A notifier that delivers the alert. Here it is a labelled mock that logs the payload and records it to `core.alerts`, so the alerting is observable and testable. In production you swap the notifier for Slack or email without touching any DAG.

The acceptance test exercises the criterion directly: a failing task fires the failure callback, and a well-formed alert with the exception text lands in `core.alerts`. A unit test also checks the payload builder in isolation.

## Failure modes (what breaks and when)

- Silent failure. Without callbacks, a failed task is only visible if someone is watching the UI. The failure callback guarantees an alert.
- Noisy or malformed alerts. If every DAG builds its own payload, alerts are inconsistent and hard to route. A shared builder makes them uniform.
- Alert on every retry. A callback firing on each retry can spam. `on_failure_callback` fires when the task instance finally fails (after retries are exhausted), so with a sensible retry policy you get one alert per real failure.
- Notifier failure. If the notifier itself throws inside a callback, it can obscure the original failure. A real implementation should catch and log its own errors so the alert path never masks the task error.

## Tradeoffs (why not just read the logs)

Reading logs works until you have more than a handful of DAGs, and then it does not scale and it is reactive. Callbacks and a notifier cost a small shared module and the discipline of wiring them into `default_args`, and give you proactive, uniform, routable alerts. The main tradeoff is alert fatigue: alert on the things that need action (failures, SLA misses), not on every success, or the signal drowns.

This DAG alerts on success too, but only to demonstrate the success callback. In practice you would usually alert on failure and on SLA misses, and record successes as metrics rather than alerts.

## Production alternatives (what a large org reaches for)

- Provider notifiers: the Slack, PagerDuty, or SMTP providers, or the Airflow `notifier` interface, in place of the mock.
- SLAs and `sla_miss_callback` so a task that is merely late (not yet failed) still raises a signal.
- Metrics and tracing: Airflow's StatsD or OpenTelemetry integration into Prometheus and Grafana for dashboards, with alerts driven by metrics rather than individual task events.
- Centralised, structured logging shipped to a log platform (ELK, Loki, Datadog) so alerts link straight to the relevant logs.

## Run it

```bash
source scripts/env.sh

# Run through the scheduler so the callbacks fire on the failing task
airflow dags backfill -s 2024-12-15 -e 2024-12-15 --reset-dagruns -y observability_hooks

# Inspect the recorded alerts
source scripts/env.sh
python -c "from airflow.providers.postgres.hooks.postgres import PostgresHook; \
print(PostgresHook('warehouse').get_records('SELECT task_id, state, exception FROM core.alerts ORDER BY alert_id DESC LIMIT 5'))"

# Or run the acceptance test
pytest tests/acceptance/test_pattern_10_observability.py -m acceptance -v
```
