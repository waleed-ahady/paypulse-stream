# Architecture

## Data journey

### 1. The transaction client creates a PaymentIntent

`scripts/create_test_payment.py` calls Stripe's PaymentIntents API with a test secret key and a documented test PaymentMethod. It can create either a successful or declined transaction. Stripe then creates an event such as `payment_intent.succeeded` or `payment_intent.payment_failed`.

### 2. Stripe CLI forwards the webhook

During local development, Stripe CLI forwards the HTTPS event from Stripe to `http://localhost:8000/webhooks/stripe`.

### 3. FastAPI verifies and normalises the event

FastAPI reads the raw body and verifies the `Stripe-Signature` header with the endpoint secret. It then converts the provider-specific payload into a smaller internal contract.

The internal event contains identifiers, amount, currency, status, timestamps, source information, and the normalised payload.

### 4. Kafka buffers the event

FastAPI publishes the event to the `payment_events` topic. The Stripe event id is used as the Kafka message key so events with the same key are routed consistently.

Kafka separates ingestion speed from processing speed. FastAPI can acknowledge the webhook after Kafka confirms delivery, while Spark processes the record independently.

### 5. Spark validates and transforms the event

Spark Structured Streaming reads Kafka continuously and performs these operations:

1. Parses the JSON contract.
2. Checks required fields and allowed event types.
3. Adds a rejection reason when data is invalid.
4. Converts minor currency units to a decimal amount.
5. Creates analytics columns such as `is_successful` and `event_time`.
6. Deduplicates by Stripe event id within the watermark window.

### 6. Spark routes the record

Valid records are upserted into PostgreSQL. Repeated event ids update the existing row rather than creating duplicates.

Invalid records are written as JSON to the local `pipeline_data` Docker volume. Each record includes its rejection reason.

### 7. Streamlit queries PostgreSQL

The dashboard selects a currency and displays totals, success rate, amount, recent events, hourly activity, and status distribution.

## Reliability decisions

- **Signature verification:** rejects forged or malformed webhook requests.
- **Kafka acknowledgement:** FastAPI flushes the produced message before returning success.
- **Event keying:** uses `event_id` as the Kafka key.
- **Database idempotency:** uses `event_id` as the PostgreSQL primary key and performs an upsert.
- **Spark checkpoints:** preserve streaming progress across container restarts.
- **Rejected-data path:** keeps invalid records for inspection rather than silently dropping them.
- **Health checks:** delay dependent services until Kafka and PostgreSQL are ready.


