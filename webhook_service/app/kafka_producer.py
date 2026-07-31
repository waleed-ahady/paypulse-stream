"""Kafka publishing logic for verified payment events."""

from __future__ import annotations

import logging
from threading import Event
from typing import Any

from confluent_kafka import KafkaError, Message, Producer

from app.schemas import PaymentEvent
from app.settings import Settings

LOGGER = logging.getLogger(__name__)


class KafkaPublishError(RuntimeError):
    """Raised when Kafka does not confirm event delivery."""


# Purpose: callers can translate Kafka failures into an HTTP retry response for Stripe.


class KafkaPublisher:
    """Small wrapper around the Confluent Kafka producer."""

    def __init__(self, settings: Settings) -> None:
        self._topic = settings.kafka_topic
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "client.id": "paypulse-webhook-api",
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "snappy",
                "linger.ms": 20,
                "message.timeout.ms": 10000,
            }
        )

    def publish(self, event: PaymentEvent) -> None:
        """Publish one event and wait for Kafka's delivery acknowledgement."""

        delivered = Event()
        delivery_error: list[KafkaError] = []

        def on_delivery(error: KafkaError | None, message: Message) -> None:
            if error is not None:
                delivery_error.append(error)
            else:
                LOGGER.info(
                    "Published event_id=%s topic=%s partition=%s offset=%s",
                    event.event_id,
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
            delivered.set()

        self._producer.produce(
            topic=self._topic,
            key=event.event_id.encode(),
            value=event.model_dump_json().encode(),
            on_delivery=on_delivery,
        )
        self._producer.poll(0)
        remaining = self._producer.flush(timeout=10)

        if remaining > 0 or not delivered.is_set() or delivery_error:
            detail = str(delivery_error[0]) if delivery_error else "delivery timed out"
            raise KafkaPublishError(detail)

    def is_ready(self) -> bool:
        """Check whether Kafka metadata can be retrieved."""

        try:
            metadata: Any = self._producer.list_topics(timeout=2)
            return bool(metadata.brokers)
        except Exception:
            LOGGER.exception("Kafka readiness check failed")
            return False

    def close(self) -> None:
        """Flush outstanding messages during service shutdown."""

        self._producer.flush(timeout=5)


# Purpose: the wrapper isolates Kafka configuration and delivery guarantees from HTTP route code.
