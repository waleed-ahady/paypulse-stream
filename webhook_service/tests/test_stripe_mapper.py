"""Unit tests for Stripe-to-PayPulse event mapping."""

from app.stripe_mapper import UnsupportedStripeEventError, map_stripe_event


def make_event(event_type: str, status: str) -> dict:
    return {
        "id": "evt_test_123",
        "type": event_type,
        "created": 1_700_000_000,
        "livemode": False,
        "data": {
            "object": {
                "id": "pi_test_123",
                "amount": 7999,
                "currency": "GBP",
                "status": status,
                "description": "Wireless headphones",
                "customer": None,
            }
        },
    }


# Purpose: the fixture mirrors only the Stripe fields used by the mapper.


def test_maps_successful_payment_intent() -> None:
    result = map_stripe_event(make_event("payment_intent.succeeded", "succeeded"))

    assert result.event_id == "evt_test_123"
    assert result.payment_id == "pi_test_123"
    assert result.amount_minor == 7999
    assert result.currency == "gbp"
    assert result.payment_status == "succeeded"
    assert result.source == "stripe"


# Purpose: this test protects the stable Kafka contract for successful payments.


def test_maps_failed_payment_intent() -> None:
    result = map_stripe_event(
        make_event("payment_intent.payment_failed", "requires_payment_method")
    )

    assert result.event_type == "payment_intent.payment_failed"
    assert result.payment_status == "requires_payment_method"


# Purpose: failed payments must enter the pipeline so the dashboard can calculate success rates.


def test_rejects_unrelated_event_type() -> None:
    try:
        map_stripe_event(make_event("customer.created", "n/a"))
    except UnsupportedStripeEventError as exc:
        assert str(exc) == "customer.created"
    else:
        raise AssertionError("UnsupportedStripeEventError was not raised")


# Purpose: unrelated Stripe events should be acknowledged but not sent through the payment pipeline.
