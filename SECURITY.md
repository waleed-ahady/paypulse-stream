# Security policy

PayPulse Stream is a local educational project and must only use Stripe test mode.

## Secret-handling rules

- Store the Stripe test secret key and webhook signing secret in `.env`.
- Use only a Stripe key beginning with `sk_test_`; never use a live key for this project.
- Never place a Stripe secret key or webhook signing secret in source code.
- Never commit `.env`.
- Rotate a secret immediately if it appears in Git history, screenshots, logs, or a public issue.
- Use synthetic or test-mode customer information only.

## Webhook security

The FastAPI service verifies the raw request body with the `Stripe-Signature` header and the configured endpoint secret before publishing anything to Kafka.


