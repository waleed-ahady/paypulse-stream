"""Idempotent PostgreSQL sink for valid Spark micro-batches."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg2
from psycopg2.extras import Json, execute_values
from pyspark.sql import DataFrame, Row

from jobs.settings import SparkSettings

LOGGER = logging.getLogger(__name__)

UPSERT_SQL = """
INSERT INTO payments (
    event_id,
    payment_id,
    event_type,
    amount_minor,
    amount_major,
    currency,
    payment_status,
    description,
    customer_id,
    livemode,
    is_successful,
    event_time,
    received_at,
    processed_at,
    raw_event
) VALUES %s
ON CONFLICT (event_id) DO UPDATE SET
    payment_id = EXCLUDED.payment_id,
    event_type = EXCLUDED.event_type,
    amount_minor = EXCLUDED.amount_minor,
    amount_major = EXCLUDED.amount_major,
    currency = EXCLUDED.currency,
    payment_status = EXCLUDED.payment_status,
    description = EXCLUDED.description,
    customer_id = EXCLUDED.customer_id,
    livemode = EXCLUDED.livemode,
    is_successful = EXCLUDED.is_successful,
    event_time = EXCLUDED.event_time,
    received_at = EXCLUDED.received_at,
    processed_at = EXCLUDED.processed_at,
    raw_event = EXCLUDED.raw_event
"""


# Purpose: the upsert makes Stripe retries safe because event_id is the conflict key.


def upsert_payment_batch(batch_df: DataFrame, batch_id: int, settings: SparkSettings) -> None:
    """Write a Spark micro-batch to PostgreSQL in bounded chunks."""

    if batch_df.isEmpty():
        LOGGER.info("Spark batch_id=%s contained no valid records", batch_id)
        return

    connection = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=10,
    )

    total = 0
    try:
        with connection, connection.cursor() as cursor:
            for chunk in _chunk_rows(batch_df.toLocalIterator(), chunk_size=1000):
                values = [_row_to_values(row) for row in chunk]
                execute_values(cursor, UPSERT_SQL, values, page_size=500)
                total += len(values)
    finally:
        connection.close()

    LOGGER.info("Upserted batch_id=%s rows=%s into PostgreSQL", batch_id, total)


# Purpose: foreachBatch calls this function once per micro-batch outside Spark's lazy plan.


def _chunk_rows(rows: Iterator[Row], chunk_size: int) -> Iterator[list[Row]]:
    """Yield bounded lists so a local portfolio job does not collect an unlimited batch."""

    chunk: list[Row] = []
    for row in rows:
        chunk.append(row)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


# Purpose: chunking is safer than converting the entire micro-batch to Pandas or one Python list.


def _row_to_values(row: Row) -> tuple[Any, ...]:
    """Convert one Spark Row into the positional PostgreSQL upsert contract."""

    raw_event = {
        "event_id": row.event_id,
        "payment_id": row.payment_id,
        "event_type": row.event_type,
        "amount_minor": row.amount_minor,
        "amount_major": row.amount_major,
        "currency": row.currency,
        "payment_status": row.payment_status,
        "description": row.description,
        "customer_id": row.customer_id,
        "livemode": row.livemode,
        "is_successful": row.is_successful,
        "event_time": row.event_time,
        "received_at": row.received_at,
        "processed_at": row.processed_at,
        "source": row.source,
    }

    return (
        row.event_id,
        row.payment_id,
        row.event_type,
        row.amount_minor,
        row.amount_major,
        row.currency,
        row.payment_status,
        row.description,
        row.customer_id,
        row.livemode,
        row.is_successful,
        row.event_time,
        row.received_at,
        row.processed_at,
        Json(raw_event, dumps=lambda value: json.dumps(value, default=_json_default)),
    )


# Purpose: preserving the normalised event as JSONB improves traceability and future reprocessing.


def _json_default(value: Any) -> str:
    """Serialise Spark-friendly date, time, and decimal values into JSON text."""

    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Unsupported JSON value: {type(value)!r}")


# Purpose: json.dumps needs explicit handling for datetime and Decimal objects from Spark rows.
