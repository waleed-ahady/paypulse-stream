"""Environment-backed settings for the webhook service."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration values supplied by Docker Compose or a local environment."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    stripe_webhook_secret: str = Field(default="whsec_replace_me")
    kafka_bootstrap_servers: str = Field(default="kafka:19092")
    kafka_topic: str = Field(default="payment_events")
    log_level: str = Field(default="INFO")

    @property
    def stripe_secret_configured(self) -> bool:
        """Return whether a real Stripe CLI or dashboard webhook secret is present."""

        return self.stripe_webhook_secret.startswith("whsec_") and (
            self.stripe_webhook_secret != "whsec_replace_me"
        )


# Purpose: centralising settings keeps secrets and hostnames out of application code.


@lru_cache
def get_settings() -> Settings:
    """Create one cached settings object per process."""

    return Settings()


# Purpose: caching avoids reparsing environment variables on every HTTP request.
