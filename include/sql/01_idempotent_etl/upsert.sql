-- Primary key upsert from staging into the final table, scoped to one logical
-- date. This is the core of the idempotency guarantee:
--
--   ON CONFLICT (transaction_id) DO UPDATE
--
-- means a second run for the same date finds the same primary keys already
-- present and updates them in place. Row count does not grow, and the business
-- columns end up identical, so the content hash is stable.
--
-- loaded_at is refreshed on every write. It is deliberately excluded from the
-- content hash, because it is an operational timestamp, not business data.

INSERT INTO core.transactions
    (transaction_id, load_date, account_id, amount, currency)
SELECT
    transaction_id, load_date, account_id, amount, currency
FROM core.stg_transactions
WHERE load_date = %(load_date)s
ON CONFLICT (transaction_id) DO UPDATE
    SET load_date  = EXCLUDED.load_date,
        account_id = EXCLUDED.account_id,
        amount     = EXCLUDED.amount,
        currency   = EXCLUDED.currency,
        loaded_at  = now();
