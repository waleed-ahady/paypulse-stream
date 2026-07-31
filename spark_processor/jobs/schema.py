"""Spark schema for the JSON values stored in Kafka."""

from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

PAYMENT_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("payment_id", StringType(), True),
        StructField("amount_minor", LongType(), True),
        StructField("currency", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("description", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("livemode", BooleanType(), True),
        StructField("provider_created_at", LongType(), True),
        StructField("received_at", StringType(), True),
        StructField("source", StringType(), True),
    ]
)

# Purpose: an explicit schema is faster and safer than asking Spark to infer streaming JSON fields.
