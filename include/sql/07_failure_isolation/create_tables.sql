-- Schema and tables for Pattern 07 (retries and failure isolation).
-- Safe to run repeatedly.

CREATE SCHEMA IF NOT EXISTS core;

-- Per-region load status. Only regions that succeeded record a row.
CREATE TABLE IF NOT EXISTS core.region_load (
    load_date  DATE        NOT NULL,
    region     TEXT        NOT NULL,
    status     TEXT        NOT NULL,
    noted_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (load_date, region)
);

-- Run-level events: the aggregate marker, and the failure-handled marker that
-- the one_failed branch writes when a region fails.
CREATE TABLE IF NOT EXISTS core.run_events (
    load_date  DATE        NOT NULL,
    event      TEXT        NOT NULL,
    detail     TEXT,
    noted_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (load_date, event)
);
