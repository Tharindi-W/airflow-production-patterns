-- Schema and tables for Pattern 08 (data quality gates).
-- Safe to run repeatedly.

CREATE SCHEMA IF NOT EXISTS core;

-- Final orders table. Columns are NOT NULL: the warehouse itself is the last
-- line of defence, but the gate should stop bad data long before here.
CREATE TABLE IF NOT EXISTS core.orders (
    order_id     TEXT           PRIMARY KEY,
    load_date    DATE           NOT NULL,
    customer_id  TEXT           NOT NULL,
    amount       NUMERIC(12, 2) NOT NULL,
    loaded_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_orders_load_date ON core.orders (load_date);

-- Staging is intentionally permissive (nullable) so a bad batch can be staged
-- and then inspected by the gate before it is allowed near the final table.
CREATE TABLE IF NOT EXISTS core.stg_orders (
    order_id     TEXT,
    load_date    DATE           NOT NULL,
    customer_id  TEXT,
    amount       NUMERIC(12, 2)
);

CREATE INDEX IF NOT EXISTS ix_stg_orders_load_date ON core.stg_orders (load_date);
