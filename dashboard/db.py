"""PostgreSQL connection helpers and parameterised dashboard queries."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pandas as pd
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor

from settings import DashboardSettings


@contextmanager
def database_connection(settings: DashboardSettings):
    """Open and reliably close one PostgreSQL connection."""

    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=5,
    )
    try:
        yield conn
    finally:
        conn.close()


# Purpose: a context manager prevents leaked database connections during Streamlit reruns.


def fetch_currencies(settings: DashboardSettings) -> list[str]:
    """Return currencies currently present in the payments table."""

    rows = _fetch_all(
        settings,
        "SELECT DISTINCT currency FROM payments ORDER BY currency",
    )
    return [str(row["currency"]) for row in rows]


# Purpose: the selector reflects actual data instead of using a hard-coded list.


def fetch_summary(settings: DashboardSettings, currency: str) -> dict[str, Any]:
    """Return headline metrics for one currency."""

    query = """
        SELECT
            COUNT(*) AS total_events,
            COUNT(*) FILTER (WHERE is_successful) AS successful_events,
            COUNT(*) FILTER (WHERE NOT is_successful) AS unsuccessful_events,
            COALESCE(SUM(amount_major) FILTER (WHERE is_successful), 0) AS successful_amount,
            COALESCE(AVG(amount_major) FILTER (WHERE is_successful), 0) AS average_successful_amount
        FROM payments
        WHERE currency = %s
    """
    rows = _fetch_all(settings, query, (currency,))
    return rows[0]


# Purpose: one aggregate query supplies all top-level dashboard cards consistently.


def fetch_status_breakdown(settings: DashboardSettings, currency: str) -> pd.DataFrame:
    """Return event counts grouped by Stripe payment status."""

    query = """
        SELECT payment_status, COUNT(*) AS event_count
        FROM payments
        WHERE currency = %s
        GROUP BY payment_status
        ORDER BY event_count DESC, payment_status
    """
    return _to_dataframe(_fetch_all(settings, query, (currency,)))


# Purpose: status counts reveal failed, processing, cancelled, and successful payment states.


def fetch_hourly_activity(settings: DashboardSettings, currency: str) -> pd.DataFrame:
    """Return the latest 24 hours of event volume and successful amount."""

    query = """
        SELECT
            DATE_TRUNC('hour', event_time) AS hour,
            COUNT(*) AS event_count,
            COALESCE(SUM(amount_major) FILTER (WHERE is_successful), 0) AS successful_amount
        FROM payments
        WHERE currency = %s
          AND event_time >= NOW() - INTERVAL '24 hours'
        GROUP BY DATE_TRUNC('hour', event_time)
        ORDER BY hour
    """
    return _to_dataframe(_fetch_all(settings, query, (currency,)))


# Purpose: hourly activity provides a simple near-real-time operational trend.


def fetch_recent_payments(
    settings: DashboardSettings,
    currency: str,
    limit: int = 25,
) -> pd.DataFrame:
    """Return the latest payment events for the selected currency."""

    safe_limit = min(max(limit, 1), 100)
    query = """
        SELECT
            event_time,
            event_type,
            payment_id,
            payment_status,
            amount_major,
            description,
            is_successful
        FROM payments
        WHERE currency = %s
        ORDER BY event_time DESC
        LIMIT %s
    """
    return _to_dataframe(_fetch_all(settings, query, (currency, safe_limit)))


# Purpose: a bounded recent-events table helps demonstrate individual records without heavy queries.


def _fetch_all(
    settings: DashboardSettings,
    query: str,
    parameters: tuple[Any, ...] = (),
) -> list[dict[str, Any]]:
    """Execute a parameterised SQL query and return dictionary rows."""

    with database_connection(settings) as conn:
        typed_connection: connection = conn
        with typed_connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, parameters)
            return [dict(row) for row in cursor.fetchall()]


# Purpose: all public query functions share one safe parameter-binding implementation.


def _to_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Create a DataFrame while preserving empty-query behaviour."""

    return pd.DataFrame(rows)


# Purpose: the helper keeps Pandas conversion out of SQL-focused functions.
