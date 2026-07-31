"""Entry point for the PayPulse Spark Structured Streaming application."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from jobs.postgres_sink import upsert_payment_batch
from jobs.settings import SparkSettings
from jobs.transforms import (
    enrich_and_validate,
    parse_kafka_records,
    select_rejected_records,
    select_valid_records,
)

LOGGER = logging.getLogger(__name__)


def build_spark_session() -> SparkSession:
    """Create a local Spark session with deterministic UTC timestamps."""

    return (
        SparkSession.builder.appName("PayPulsePaymentStream")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# Purpose: UTC prevents timestamp differences between developer laptops and Docker containers.


def main() -> None:
    settings = SparkSettings.from_environment()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    spark = build_spark_session()
    spark.sparkContext.setLogLevel(settings.log_level.upper())

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = parse_kafka_records(kafka_df)
    enriched = enrich_and_validate(parsed)
    valid = select_valid_records(enriched)
    rejected = select_rejected_records(enriched)

    valid_query = (
        valid.writeStream.outputMode("append")
        .option("checkpointLocation", f"{settings.checkpoint_root}/postgres")
        .foreachBatch(lambda frame, batch_id: upsert_payment_batch(frame, batch_id, settings))
        .queryName("paypulse-postgres-sink")
        .start()
    )

    rejected_query = (
        rejected.writeStream.format("json")
        .outputMode("append")
        .option("path", settings.rejected_path)
        .option("checkpointLocation", f"{settings.checkpoint_root}/rejected")
        .partitionBy("rejection_date")
        .queryName("paypulse-rejected-sink")
        .start()
    )

    LOGGER.info(
        "Started queries valid_id=%s rejected_id=%s topic=%s",
        valid_query.id,
        rejected_query.id,
        settings.kafka_topic,
    )

    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in spark.streams.active:
            query.stop()
        spark.stop()


# Purpose: main wires the parsing, transformation, valid sink, and rejected sink together.


if __name__ == "__main__":
    main()
