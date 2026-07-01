-- Schema and table for Pattern 05 (slow upstream handling).
-- Safe to run repeatedly.

CREATE SCHEMA IF NOT EXISTS core;

-- Records which upstream sources reported in for a logical date, and the final
-- report marker. Lets the acceptance test see that the fast source and the
-- report completed even when the slow source timed out.
CREATE TABLE IF NOT EXISTS core.source_status (
    load_date  DATE        NOT NULL,
    source     TEXT        NOT NULL,
    status     TEXT        NOT NULL,
    noted_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (load_date, source)
);
