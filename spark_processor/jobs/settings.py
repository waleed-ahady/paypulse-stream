"""Environment-backed settings for the Spark streaming job."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SparkSettings:
    """Immutable runtime settings used by the Spark driver and PostgreSQL sink."""

    kafka_bootstrap_servers: str
    kafka_topic: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    checkpoint_root: str
    rejected_path: str
    log_level: str

    @classmethod
    def from_environment(cls) -> "SparkSettings":
        """Build settings from Docker Compose environment variables."""

        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
            kafka_topic=os.getenv("KAFKA_TOPIC", "payment_events"),
            postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_db=os.getenv("POSTGRES_DB", "paypulse"),
            postgres_user=os.getenv("POSTGRES_USER", "paypulse"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "change_me_locally"),
            checkpoint_root=os.getenv("SPARK_CHECKPOINT_ROOT", "/app/data/checkpoints"),
            rejected_path=os.getenv("SPARK_REJECTED_PATH", "/app/data/rejected"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )


# Purpose: one typed object prevents environment-variable lookups from being scattered across jobs.
