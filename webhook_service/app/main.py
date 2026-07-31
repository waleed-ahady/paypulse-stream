"""FastAPI application that verifies Stripe webhooks and publishes payment events."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import stripe
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.concurrency import run_in_threadpool

from app.kafka_producer import KafkaPublishError, KafkaPublisher
from app.settings import Settings, get_settings
from app.stripe_mapper import UnsupportedStripeEventError, map_stripe_event

LOGGER = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create shared settings and Kafka resources for the process lifetime."""

    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    publisher = KafkaPublisher(settings)

    app.state.settings = settings
    app.state.publisher = publisher
    yield
    publisher.close()


# Purpose: one producer is reused across requests instead of reconnecting for every webhook.


app = FastAPI(
    title="PayPulse Webhook API",
    version="1.0.0",
    description="Verifies Stripe test-mode webhooks and publishes payment events to Kafka.",
    lifespan=lifespan,
)


@app.get("/health/live")
def live() -> dict[str, str]:
    """Return a process-level liveness response."""

    return {"status": "alive"}


# Purpose: liveness confirms that the web server process is running.


@app.get("/health/ready")
def ready(request: Request) -> dict[str, object]:
    """Report whether Stripe configuration and Kafka are ready for traffic."""

    settings: Settings = request.app.state.settings
    publisher: KafkaPublisher = request.app.state.publisher
    kafka_ready = publisher.is_ready()
    stripe_ready = settings.stripe_secret_configured

    if not kafka_ready or not stripe_ready:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "status": "not_ready",
                "kafka": kafka_ready,
                "stripe_secret": stripe_ready,
            },
        )

    return {"status": "ready", "kafka": True, "stripe_secret": True}


# Purpose: readiness prevents false confidence when Kafka or the webhook secret is missing.


@app.post("/webhooks/stripe")
async def stripe_webhook(request: Request) -> dict[str, str]:
    """Verify a Stripe webhook, map it, and publish supported events to Kafka."""

    settings: Settings = request.app.state.settings
    publisher: KafkaPublisher = request.app.state.publisher
    signature = request.headers.get("stripe-signature")

    if not signature:
        raise HTTPException(status_code=400, detail="Missing Stripe-Signature header")

    payload = await request.body()

    try:
        event = stripe.Webhook.construct_event(
            payload=payload,
            sig_header=signature,
            secret=settings.stripe_webhook_secret,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc
    except stripe.error.SignatureVerificationError as exc:
        raise HTTPException(status_code=400, detail="Invalid Stripe signature") from exc

    try:
        event_payload = event.to_dict()
        event_type = str(event_payload.get("type", ""))
        supported_event_types = {
            "payment_intent.succeeded",
            "payment_intent.payment_failed",
            "payment_intent.processing",
            "payment_intent.canceled",
        }
        if event_type not in supported_event_types:
            return {
                "status": "ignored",
                "event_type": event_type,
            }

        payment_event = map_stripe_event(event_payload)
    except UnsupportedStripeEventError:
        LOGGER.info("Acknowledged unsupported Stripe event type=%s", event.get("type"))
        return {"status": "ignored", "event_id": str(event.get("id", "unknown"))}

    try:
        await run_in_threadpool(publisher.publish, payment_event)
    except KafkaPublishError as exc:
        LOGGER.exception("Kafka publication failed for event_id=%s", payment_event.event_id)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka unavailable; Stripe should retry this webhook",
        ) from exc

    return {"status": "published", "event_id": payment_event.event_id}


# Purpose: verification, mapping, and Kafka publishing remain separate from HTTP routing.
