-- Schema and table for Pattern 06 (dynamic task mapping).
-- Safe to run repeatedly.

CREATE SCHEMA IF NOT EXISTS core;

-- One row per input file processed. The primary key (load_date, input_name)
-- means re-processing an input upserts in place, so the mapped tasks stay
-- idempotent under retries.
CREATE TABLE IF NOT EXISTS core.file_ingest (
    load_date   DATE        NOT NULL,
    input_name  TEXT        NOT NULL,
    value       INTEGER     NOT NULL,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (load_date, input_name)
);
