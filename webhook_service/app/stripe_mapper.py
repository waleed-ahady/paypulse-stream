"""Convert verified Stripe events into the PayPulse Kafka contract."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from app.schemas import PaymentEvent

SUPPORTED_EVENT_TYPES = {
    "payment_intent.succeeded",
    "payment_intent.payment_failed",
    "payment_intent.processing",
    "payment_intent.canceled",
}


class UnsupportedStripeEventError(ValueError):
    """Raised when the webhook is valid but outside the project's event scope."""


# Purpose: the API can acknowledge unsupported events without publishing them.


def map_stripe_event(event: Mapping[str, Any]) -> PaymentEvent:
    """Map a verified Stripe Event dictionary to the internal payment schema."""

    event_type = str(event.get("type", ""))
    if event_type not in SUPPORTED_EVENT_TYPES:
        raise UnsupportedStripeEventError(event_type)

    data = event.get("data") or {}
    payment_intent = data.get("object") or {}

    return PaymentEvent(
        event_id=str(event["id"]),
        event_type=event_type,
        payment_id=str(payment_intent["id"]),
        amount_minor=int(payment_intent.get("amount", 0)),
        currency=str(payment_intent.get("currency", "")).lower(),
        payment_status=str(payment_intent.get("status", "unknown")),
        description=payment_intent.get("description"),
        customer_id=_optional_string(payment_intent.get("customer")),
        livemode=bool(event.get("livemode", False)),
        provider_created_at=int(event.get("created", 0)),
        received_at=datetime.now(UTC),
    )


# Purpose: normalisation removes most Stripe-specific nesting before the record reaches Kafka.


def _optional_string(value: Any) -> str | None:
    """Convert optional Stripe identifiers to strings without producing the text 'None'."""

    if value is None:
        return None
    return str(value)


# Purpose: optional and expandable Stripe fields produce predictable output.
