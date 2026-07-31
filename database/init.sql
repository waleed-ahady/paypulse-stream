-- PayPulse Stream database schema.
-- This file runs automatically only when PostgreSQL creates a new data volume.

CREATE TABLE IF NOT EXISTS payments (
    event_id TEXT PRIMARY KEY,
    payment_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    amount_minor BIGINT NOT NULL CHECK (amount_minor >= 0),
    amount_major NUMERIC(18, 3) NOT NULL CHECK (amount_major >= 0),
    currency VARCHAR(3) NOT NULL,
    payment_status TEXT NOT NULL,
    description TEXT,
    customer_id TEXT,
    livemode BOOLEAN NOT NULL DEFAULT FALSE,
    is_successful BOOLEAN NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_event JSONB NOT NULL
);

-- Purpose: the primary key makes repeated delivery of the same Stripe event idempotent.

CREATE INDEX IF NOT EXISTS idx_payments_event_time
    ON payments (event_time DESC);

CREATE INDEX IF NOT EXISTS idx_payments_currency_status
    ON payments (currency, payment_status);

CREATE INDEX IF NOT EXISTS idx_payments_payment_id
    ON payments (payment_id);

-- Purpose: these indexes speed up dashboard filters, recent-event queries, and payment lookups.

CREATE OR REPLACE VIEW payment_daily_summary AS
SELECT
    DATE_TRUNC('day', event_time) AS payment_day,
    currency,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE is_successful) AS successful_count,
    COALESCE(SUM(amount_major) FILTER (WHERE is_successful), 0) AS successful_amount
FROM payments
GROUP BY DATE_TRUNC('day', event_time), currency;

-- Purpose: the view provides a reusable daily aggregate for analytics and future reporting.
