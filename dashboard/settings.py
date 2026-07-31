"""Environment-backed settings for the Streamlit dashboard."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class DashboardSettings(BaseSettings):
    """Database and refresh settings provided by Docker Compose."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "paypulse"
    postgres_user: str = "paypulse"
    postgres_password: str = "potsdam2026"
    dashboard_refresh_seconds: int = 5
    dashboard_default_currency: str = "gbp"


# Purpose: configuration remains outside Streamlit rendering and SQL code.


@lru_cache
def get_settings() -> DashboardSettings:
    """Return one cached settings object per dashboard process."""

    return DashboardSettings()


# Purpose: caching avoids rebuilding the same environment-backed object on each rerun.
