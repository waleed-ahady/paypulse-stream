# File guide and execution order

## Root files

- `README.md`: setup instructions, commands, architecture summary, and GitHub publication checklist.
- `.env.example`: safe template for local environment variables. Copy it to `.env`.
- `.gitignore`: prevents secrets, caches, local data, and editor files from entering Git.
- `.dockerignore`: keeps unnecessary files out of Docker build contexts.
- `docker-compose.yml`: starts Kafka, the Kafka topic initializer, PostgreSQL, FastAPI, Spark, and Streamlit.
- `Makefile`: short aliases for common Docker, test, lint, and validation commands.
- `pyproject.toml`: central Ruff and Pytest configuration.
- `requirements-dev.txt`: local testing and linting tools.
- `LICENSE`: MIT licence for public GitHub use.
- `SECURITY.md`: rules for secrets, test data, and local-only networking.

## Database

- `database/init.sql`: creates the `payments` table, indexes, constraints, and daily summary view when PostgreSQL first starts.

## Webhook service

- `webhook_service/Dockerfile`: builds the FastAPI container using a small Python image and a non-root user.
- `webhook_service/requirements.txt`: runtime packages for FastAPI, Stripe, settings, and Kafka.
- `webhook_service/app/settings.py`: reads and validates environment variables.
- `webhook_service/app/schemas.py`: defines the internal Kafka event contract.
- `webhook_service/app/stripe_mapper.py`: converts Stripe events into the internal contract.
- `webhook_service/app/kafka_producer.py`: owns the Kafka producer and delivery checks.
- `webhook_service/app/main.py`: defines application startup, health routes, and the Stripe webhook endpoint.
- `webhook_service/tests/test_stripe_mapper.py`: verifies successful and failed event mapping.

## Spark processor

- `spark_processor/Dockerfile`: builds the Spark container and installs the PostgreSQL Python driver.
- `spark_processor/jobs/settings.py`: reads Kafka, PostgreSQL, checkpoint, and rejected-data settings.
- `spark_processor/jobs/schema.py`: defines the JSON schema used to parse Kafka values.
- `spark_processor/jobs/transforms.py`: adds validation, rejection reasons, timestamps, amounts, and success flags.
- `spark_processor/jobs/postgres_sink.py`: upserts each valid Spark micro-batch into PostgreSQL.
- `spark_processor/jobs/payment_stream.py`: creates Spark, reads Kafka, splits valid and rejected data, starts both output streams, and waits for termination.
- `spark_processor/tests/test_currency_helpers.py`: tests pure currency conversion rules without starting Spark.

## Dashboard

- `dashboard/Dockerfile`: builds the Streamlit container using a non-root user.
- `dashboard/requirements.txt`: Streamlit, PostgreSQL, Pandas, and auto-refresh packages.
- `dashboard/settings.py`: reads dashboard and PostgreSQL settings.
- `dashboard/db.py`: contains parameterised analytics queries and connection helpers.
- `dashboard/formatting.py`: formats currencies for display without converting their values.
- `dashboard/app.py`: renders filters, metrics, charts, and recent transactions.

## Scripts and automation

- `scripts/requirements.txt`: packages used by the local Stripe API and signed-event helpers.
- `scripts/create_test_payment.py`: creates and confirms successful or declined test PaymentIntents through the Stripe API.
- `scripts/send_demo_event.py`: creates and signs a Stripe-shaped local event for development testing.
- `webhook_service/app/__init__.py`: marks the webhook application directory as a Python package.
- `spark_processor/jobs/__init__.py`: marks the Spark jobs directory as a Python package.
- `.github/workflows/ci.yml`: runs linting, unit tests, Python compilation, and Docker Compose validation for pushes and pull requests.

## Runtime execution order

1. `docker-compose.yml` starts `kafka` and `postgres`.
2. PostgreSQL runs `database/init.sql` when its data volume is new.
3. `kafka-init` creates the `payment_events` topic.
4. `webhook-api` starts `app/main.py` and waits for Stripe webhooks.
5. `create_test_payment.py` creates a Stripe test PaymentIntent; Stripe CLI forwards its event to `/webhooks/stripe`.
6. `stripe_mapper.py` creates the internal event.
7. `kafka_producer.py` publishes the event to Kafka.
8. `payment_stream.py` reads the event from Kafka.
9. `transforms.py` validates and enriches the event.
10. `postgres_sink.py` upserts valid rows; Spark writes invalid rows as JSON.
11. `dashboard/app.py` calls `dashboard/db.py` and renders PostgreSQL results.
