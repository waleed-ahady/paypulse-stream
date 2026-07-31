"""Create and confirm a Stripe test-mode PaymentIntent through the Stripe API.

Run Stripe CLI webhook forwarding before using this script. Stripe will create
an actual test-mode PaymentIntent and then deliver the resulting webhook event
to the PayPulse FastAPI service.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

import stripe
from dotenv import load_dotenv

PAYMENT_METHODS = {
    "success": "pm_card_visa",
    "declined": "pm_card_visa_chargeDeclined",
}


# Purpose: Stripe's documented test PaymentMethods create predictable test outcomes.


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=PAYMENT_METHODS, default="success")
    parser.add_argument("--amount", type=int, default=7999, help="Amount in minor units.")
    parser.add_argument("--currency", default="gbp")
    parser.add_argument("--description", default="PayPulse API test payment")
    return parser.parse_args()


# Purpose: command-line options make portfolio demonstrations repeatable.


def create_payment_intent(args: argparse.Namespace) -> Any:
    """Call Stripe's PaymentIntents API with a test payment method."""

    api_key = os.getenv("STRIPE_SECRET_KEY", "")
    if not api_key.startswith("sk_test_") or api_key == "sk_test_replace_me":
        raise RuntimeError(
            "Set STRIPE_SECRET_KEY in .env to a Stripe test secret key beginning with sk_test_."
        )

    stripe.api_key = api_key
    return stripe.PaymentIntent.create(
        amount=args.amount,
        currency=args.currency.lower(),
        payment_method=PAYMENT_METHODS[args.scenario],
        payment_method_types=["card"],
        confirm=True,
        error_on_requires_action=True,
        description=args.description,
        metadata={
            "project": "paypulse-stream",
            "scenario": args.scenario,
        },
    )


# Purpose: confirm=True creates the transaction and its final webhook event in one API call.


def print_declined_result(error: stripe.error.CardError) -> None:
    """Print the PaymentIntent details returned with an expected declined test payment."""

    error_body = error.json_body or {}
    error_details = error_body.get("error", {})
    payment_intent = error_details.get("payment_intent", {})

    print("Stripe created the expected declined test payment.")
    print(f"PaymentIntent: {payment_intent.get('id', 'unknown')}")
    print(f"Status: {payment_intent.get('status', 'unknown')}")
    print(f"Decline message: {error_details.get('message', str(error))}")


# Purpose: a declined API response is a valid test scenario, not a project failure.


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env")
    args = parse_args()

    try:
        payment_intent = create_payment_intent(args)
    except stripe.error.CardError as exc:
        if args.scenario == "declined":
            print_declined_result(exc)
            return
        raise

    print("Stripe test payment created successfully.")
    print(f"PaymentIntent: {payment_intent.id}")
    print(f"Status: {payment_intent.status}")
    print(f"Amount: {payment_intent.amount} {payment_intent.currency.upper()} minor units")


# Purpose: main loads local secrets, performs the API call, and reports the resulting state.


if __name__ == "__main__":
    main()
