-- Idempotent schema and table definitions for Pattern 01.
-- Safe to run repeatedly: every statement uses IF NOT EXISTS.

CREATE SCHEMA IF NOT EXISTS core;

-- Final target table. transaction_id is the primary key, which is what makes
-- the upsert (ON CONFLICT) idempotent: writing the same transaction twice
-- updates the row in place instead of creating a duplicate.
CREATE TABLE IF NOT EXISTS core.transactions (
    transaction_id  TEXT           PRIMARY KEY,
    load_date       DATE           NOT NULL,
    account_id      TEXT           NOT NULL,
    amount          NUMERIC(12, 2) NOT NULL,
    currency        TEXT           NOT NULL,
    loaded_at       TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_transactions_load_date
    ON core.transactions (load_date);

-- Staging table for one logical date's extract before it is upserted.
-- No primary key: staging holds the raw extract for a single run.
CREATE TABLE IF NOT EXISTS core.stg_transactions (
    transaction_id  TEXT           NOT NULL,
    load_date       DATE           NOT NULL,
    account_id      TEXT           NOT NULL,
    amount          NUMERIC(12, 2) NOT NULL,
    currency        TEXT           NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_stg_transactions_load_date
    ON core.stg_transactions (load_date);
