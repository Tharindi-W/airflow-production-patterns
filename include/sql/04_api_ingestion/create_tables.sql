-- Schema and table for Pattern 04 (API ingestion with throttling).
-- Safe to run repeatedly.

CREATE SCHEMA IF NOT EXISTS core;

-- Events pulled from the (mock) paginated API. event_id is the primary key so
-- ingestion is idempotent under retries: re-pulling a page upserts in place.
CREATE TABLE IF NOT EXISTS core.api_events (
    event_id   TEXT        PRIMARY KEY,
    page       INTEGER     NOT NULL,
    payload    TEXT        NOT NULL,
    load_date  DATE        NOT NULL,
    loaded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_api_events_load_date
    ON core.api_events (load_date);
