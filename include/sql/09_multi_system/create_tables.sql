-- Schema and tables for Pattern 09 (multi-system orchestration).
-- Safe to run repeatedly.

CREATE SCHEMA IF NOT EXISTS core;

-- Curated orders landed at the end of the chain.
CREATE TABLE IF NOT EXISTS core.curated_orders (
    order_id   TEXT           PRIMARY KEY,
    load_date  DATE           NOT NULL,
    product    TEXT           NOT NULL,
    revenue    NUMERIC(14, 2) NOT NULL,
    loaded_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_curated_orders_load_date
    ON core.curated_orders (load_date);

-- Marker rows written by the (mock) BI refresh step, so the final hop is
-- observable end to end.
CREATE TABLE IF NOT EXISTS core.bi_refreshes (
    load_date    DATE        NOT NULL,
    dashboard    TEXT        NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (load_date, dashboard)
);
