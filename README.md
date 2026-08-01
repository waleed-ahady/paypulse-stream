# PayPulse Stream

PayPulse Stream is a real-time payment data engineering project that shows how payment events can move through a complete data pipeline.

The project creates test payments using Stripe, receives the payment events through a FastAPI webhook, sends them to Apache Kafka, processes them with Apache Spark Structured Streaming, stores valid records in PostgreSQL, and displays the results in a Streamlit dashboard.

The entire project runs locally with Docker Compose.

## How the project works

The pipeline starts when a test payment is created with Stripe.

Stripe sends a webhook event to the FastAPI service. FastAPI checks that the event is genuine, extracts the useful payment information, and sends the event to Kafka.

Kafka stores the event until Spark is ready to process it.

Spark reads the event, checks whether the data is valid, converts the payment amount into a readable format, removes duplicate events, and sends the clean record to PostgreSQL.

If a record is invalid, Spark stores it in the rejected-data folder instead of adding it to the database.

The Streamlit dashboard reads the processed data from PostgreSQL and displays payment statistics.

## Architecture


Stripe test payment

        |
        v
Stripe webhook

        |
        v
FastAPI

        |
        v
Apache Kafka

        |
        v
Apache Spark Structured Streaming

      /   \
     v     v
PostgreSQL  Rejected records

     |
     v
Streamlit dashboard


Technologies used:

Stripe is used to create realistic test payments.
FastAPI receives and verifies Stripe webhook events.
Apache Kafka stores and transports payment events.
Apache Spark Structured Streaming validates and transforms the events.
PostgreSQL stores clean payment records.
Streamlit displays payment analytics.
Docker Compose runs all services locally.
GitHub Actions checks the code and runs tests automatically.
Main features

The project includes:

Real Stripe test transactions
Successful and declined payment scenarios
Secure webhook signature verification
Real-time event streaming with Kafka
Streaming data processing with Spark
Duplicate-event protection
Data validation
Rejected-record handling
PostgreSQL storage
A live Streamlit dashboard
Automated testing with GitHub Actions

Project structure:
paypulse-stream/
├── dashboard/
├── database/
├── data/
├── docs/
├── scripts/
├── spark_processor/
├── webhook_service/
├── .github/
├── docker-compose.yml
├── README.md
└── .env.example

The main folders are:

webhook_service/ contains the FastAPI webhook application.
spark_processor/ contains the Spark streaming logic.
dashboard/ contains the Streamlit dashboard.
database/ contains the PostgreSQL database setup.
scripts/ contains helper scripts for creating test transactions.
data/ stores Kafka data, Spark checkpoints, and rejected records.
docs/ contains detailed project documentation.
.github/workflows/ contains the GitHub Actions workflow.
Requirements

Before running the project, install:

Docker
Docker Compose
Python 3.12 or newer
Stripe CLI
Git

You also need a Stripe account with test mode enabled.

Setup

Clone the repository:

git clone https://github.com/waleed-ahady/paypulse-stream.git
cd paypulse-stream

Create the environment file:

cp .env.example .env

Add your Stripe test secret key to .env:

STRIPE_SECRET_KEY=sk_test_...

Start the project:

docker compose up --build -d

Check that the services are running:

docker compose ps
Stripe webhook setup

Log in to Stripe CLI:

stripe login

Start forwarding webhook events:

stripe listen --forward-to localhost:8000/webhooks/stripe

Stripe CLI will display a webhook secret starting with:

whsec_

Add that value to .env:

STRIPE_WEBHOOK_SECRET=whsec_...

Restart the webhook service:

docker compose restart webhook-api
Create test transactions

Install the script dependencies:

python -m pip install -r scripts/requirements.txt

Create a successful test payment:

python scripts/create_test_payment.py --scenario success --amount 9999 --currency gbp

Create a declined test payment:

python scripts/create_test_payment.py --scenario declined --amount 5449 --currency gbp

The amount uses the smallest currency unit.

For example:

4500 GBP = £45.00
Open the application

The Streamlit dashboard is available at:

http://localhost:8501

The FastAPI documentation is available at:

http://localhost:8000/docs

The health-check endpoint is available at:

http://localhost:8000/health/live
Dashboard

The dashboard shows:

Total payment events
Successful payments
Failed payments
Payment success rate
Payment amounts by currency
Recent transactions
Payment activity over time
Data validation

Spark checks each event before saving it.

A record may be rejected when:

The event ID is missing
The payment ID is missing
The amount is invalid
The currency code is invalid
The payment status is missing
The timestamp is invalid
The event type is unsupported

Rejected records are stored in:

data/rejected/

Each rejected record includes a reason explaining why it failed validation.

Testing

Install the development dependencies:

python -m pip install -r requirements-dev.txt

Run the tests:

python -m pytest

Run the code-quality checks:

python -m ruff check .

Validate the Docker Compose file:

docker compose config --quiet
GitHub Actions

The GitHub Actions workflow runs automatically when code is pushed to the main branch or when a pull request is opened.

It checks:

Python code quality
Python syntax
Unit tests
Docker Compose configuration

The workflow file is located at:

.github/workflows/ci.yml


Stopping the project:

Stop the project without deleting stored data:

docker compose down

To start it again:

docker compose up -d

Avoid using this command unless you intentionally want to delete Docker volumes:

docker compose down -v
Documentation

More detailed information is available in:

docs/ARCHITECTURE.md
docs/FILE_GUIDE.md
SECURITY.md

Project scope:

PayPulse Stream is designed as a local portfolio project.
It uses one Kafka broker, one Spark process, local PostgreSQL, local Docker networking, and Stripe test-mode payments.
A production version would require stronger security, encrypted connections, monitoring, backups, multiple Kafka brokers, and scalable Spark infrastructure.


License

This project is licensed under the MIT License.
