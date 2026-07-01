-- Schema and table for Pattern 10 (observability and monitoring hooks).
-- Safe to run repeatedly.

CREATE SCHEMA IF NOT EXISTS core;

-- Sink for the (mock) notifier. Every alert the callbacks would send is
-- recorded here so it is observable and testable.
CREATE TABLE IF NOT EXISTS core.alerts (
    alert_id      BIGSERIAL   PRIMARY KEY,
    dag_id        TEXT,
    task_id       TEXT,
    run_id        TEXT,
    logical_date  TEXT,
    state         TEXT,
    try_number    INTEGER,
    exception     TEXT,
    channel       TEXT,
    payload       JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_alerts_dag_task ON core.alerts (dag_id, task_id);
