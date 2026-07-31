"""Internal event contracts published from FastAPI to Kafka."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class PaymentEvent(BaseModel):
    """Provider-neutral payment event consumed by the Spark processor."""

    model_config = ConfigDict(extra="forbid")

    event_id: str
    event_type: str
    payment_id: str
    amount_minor: int = Field(ge=0)
    currency: str = Field(min_length=3, max_length=3)
    payment_status: str
    description: str | None = None
    customer_id: str | None = None
    livemode: bool
    provider_created_at: int = Field(ge=0)
    received_at: datetime
    source: str = "stripe"


# Purpose: this schema is the stable boundary between Stripe-specific ingestion and Spark.
