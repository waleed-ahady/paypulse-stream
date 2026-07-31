"""Spark parsing, validation, enrichment, and routing transformations."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from jobs.currency import THREE_DECIMAL_CURRENCIES, ZERO_DECIMAL_CURRENCIES
from jobs.schema import PAYMENT_EVENT_SCHEMA

SUPPORTED_EVENT_TYPES = [
    "payment_intent.succeeded",
    "payment_intent.payment_failed",
    "payment_intent.processing",
    "payment_intent.canceled",
]


# Purpose: keeping the supported list explicit makes pipeline scope visible during code review.


def parse_kafka_records(kafka_df: DataFrame) -> DataFrame:
    """Decode Kafka key/value bytes and parse the JSON event contract."""

    return (
        kafka_df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_value"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .withColumn("payload", F.from_json(F.col("raw_value"), PAYMENT_EVENT_SCHEMA))
        .select(
            "kafka_key",
            "raw_value",
            "kafka_timestamp",
            "kafka_partition",
            "kafka_offset",
            "payload.*",
        )
    )


# Purpose: Kafka metadata stays attached so rejected records can be traced to their source.


def enrich_and_validate(records: DataFrame) -> DataFrame:
    """Add validation results, timestamps, amount conversion, and analytics fields."""

    divisor = (
        F.when(F.lower(F.col("currency")).isin(*ZERO_DECIMAL_CURRENCIES), F.lit(1))
        .when(F.lower(F.col("currency")).isin(*THREE_DECIMAL_CURRENCIES), F.lit(1000))
        .otherwise(F.lit(100))
    )

    rejection_reason = F.concat_ws(
        "; ",
        F.when(F.col("event_id").isNull() | (F.trim(F.col("event_id")) == ""), "missing event_id"),
        F.when(
            F.col("payment_id").isNull() | (F.trim(F.col("payment_id")) == ""),
            "missing payment_id",
        ),
        F.when(F.col("amount_minor").isNull(), "missing amount_minor"),
        F.when(F.col("amount_minor") < 0, "amount_minor must be non-negative"),
        F.when(
            F.col("currency").isNull() | (F.length(F.trim(F.col("currency"))) != 3),
            "currency must contain three characters",
        ),
        F.when(
            F.col("event_type").isNull() | (F.trim(F.col("event_type")) == ""),
            "missing event_type",
        ),
        F.when(
            F.col("event_type").isNotNull()
            & ~F.col("event_type").isin(SUPPORTED_EVENT_TYPES),
            "unsupported event_type",
        ),
        F.when(
            F.col("payment_status").isNull() | (F.trim(F.col("payment_status")) == ""),
            "missing payment_status",
        ),
        F.when(F.col("provider_created_at").isNull(), "missing provider_created_at"),
        F.when(F.col("provider_created_at") < 0, "provider_created_at must be non-negative"),
        F.when(F.col("received_at").isNull(), "missing received_at"),
    )

    with_timestamps = (
        records.withColumn("currency", F.lower(F.col("currency")))
        .withColumn("event_time", F.to_timestamp(F.from_unixtime(F.col("provider_created_at"))))
        .withColumn("received_at_ts", F.to_timestamp(F.col("received_at")))
    )

    timestamp_rejection = F.concat_ws(
        "; ",
        rejection_reason,
        F.when(
            F.col("provider_created_at").isNotNull() & F.col("event_time").isNull(),
            "invalid provider_created_at",
        ),
        F.when(
            F.col("received_at").isNotNull() & F.col("received_at_ts").isNull(),
            "invalid received_at",
        ),
    )

    return (
        with_timestamps
        .withColumn(
            "amount_major",
            (F.col("amount_minor") / divisor).cast(DecimalType(18, 3)),
        )
        .withColumn("is_successful", F.col("event_type") == "payment_intent.succeeded")
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("rejection_reason", timestamp_rejection)
        .withColumn("is_valid", F.col("rejection_reason") == "")
    )


# Purpose: validation produces an explanation instead of silently discarding malformed records.


def select_valid_records(records: DataFrame) -> DataFrame:
    """Return valid, deduplicated records with the PostgreSQL column contract."""

    valid = records.filter(F.col("is_valid"))

    return (
        valid.withWatermark("event_time", "1 day")
        .dropDuplicates(["event_id"])
        .select(
            "event_id",
            "payment_id",
            "event_type",
            "amount_minor",
            "amount_major",
            "currency",
            "payment_status",
            "description",
            "customer_id",
            "livemode",
            "is_successful",
            "event_time",
            F.col("received_at_ts").alias("received_at"),
            "processed_at",
            "source",
        )
    )


# Purpose: the selected columns exactly match the database sink's expected values.


def select_rejected_records(records: DataFrame) -> DataFrame:
    """Return invalid records with diagnostic and Kafka trace information."""

    return (
        records.filter(~F.col("is_valid"))
        .withColumn("rejection_date", F.to_date(F.col("processed_at")))
        .select(
            "rejection_date",
            "rejection_reason",
            "raw_value",
            "kafka_key",
            "kafka_timestamp",
            "kafka_partition",
            "kafka_offset",
            "processed_at",
        )
    )


# Purpose: rejected output contains enough context to debug bad producers or schema changes.
