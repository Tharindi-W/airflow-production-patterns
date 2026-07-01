-- Schema and table for Pattern 02 (backfill-safe pipeline).
-- Safe to run repeatedly.

CREATE SCHEMA IF NOT EXISTS core;

-- Daily sales rolled up per region. The primary key is (load_date, region),
-- so each logical date owns a distinct set of rows: its partition. A run for
-- one date can never collide with another date's rows on the primary key.
CREATE TABLE IF NOT EXISTS core.daily_sales (
    load_date     DATE           NOT NULL,
    region        TEXT           NOT NULL,
    txn_count     INTEGER        NOT NULL,
    total_amount  NUMERIC(14, 2) NOT NULL,
    loaded_at     TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (load_date, region)
);

CREATE INDEX IF NOT EXISTS ix_daily_sales_load_date
    ON core.daily_sales (load_date);
