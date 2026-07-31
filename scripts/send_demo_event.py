"""Send a locally signed Stripe-shaped event to the webhook service.

This script is a development helper. It does not call the Stripe API and should
not replace a real Stripe CLI demonstration in the final portfolio project.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status", choices=["succeeded", "failed"], default="succeeded")
    parser.add_argument("--amount", type=int, default=7999, help="Amount in minor units.")
    parser.add_argument("--currency", default="gbp")
    parser.add_argument("--url", default="http://localhost:8000/webhooks/stripe")
    return parser.parse_args()


# Purpose: command-line arguments let you quickly create different payment scenarios.


def build_event(status: str, amount: int, currency: str) -> dict[str, Any]:
    now = int(time.time())
    event_type = (
        "payment_intent.succeeded"
        if status == "succeeded"
        else "payment_intent.payment_failed"
    )
    payment_status = "succeeded" if status == "succeeded" else "requires_payment_method"

    return {
        "id": f"evt_demo_{uuid.uuid4().hex}",
        "object": "event",
        "type": event_type,
        "created": now,
        "livemode": False,
        "data": {
            "object": {
                "id": f"pi_demo_{uuid.uuid4().hex}",
                "object": "payment_intent",
                "amount": amount,
                "currency": currency.lower(),
                "status": payment_status,
                "description": "PayPulse demo transaction",
                "customer": None,
                "metadata": {"source": "local_demo"},
            }
        },
        "request": {"id": None, "idempotency_key": None},
        "pending_webhooks": 1,
    }


# Purpose: the payload follows the Stripe event shape used by the webhook mapper.


def create_signature(payload: bytes, secret: str, timestamp: int) -> str:
    signed_payload = f"{timestamp}.".encode() + payload
    signature = hmac.new(secret.encode(), signed_payload, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={signature}"


# Purpose: Stripe's Python SDK can verify this header in the same way as a CLI event.


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env")
    args = parse_args()
    secret = os.getenv("STRIPE_WEBHOOK_SECRET", "whsec_replace_me")
    event = build_event(args.status, args.amount, args.currency)
    payload = json.dumps(event, separators=(",", ":")).encode()
    timestamp = int(time.time())
    signature = create_signature(payload, secret, timestamp)

    response = httpx.post(
        args.url,
        content=payload,
        headers={
            "Content-Type": "application/json",
            "Stripe-Signature": signature,
        },
        timeout=10,
    )
    response.raise_for_status()

    print(f"Sent at {datetime.now(UTC).isoformat()}")
    print(json.dumps(response.json(), indent=2))


# Purpose: main signs and sends the event, then prints FastAPI's acknowledgement.


if __name__ == "__main__":
    main()
