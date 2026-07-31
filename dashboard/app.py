"""Streamlit analytics dashboard for PayPulse payment events."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import streamlit as st
from db import (
    fetch_currencies,
    fetch_hourly_activity,
    fetch_recent_payments,
    fetch_status_breakdown,
    fetch_summary,
)
from formatting import format_money
from settings import get_settings

SETTINGS = get_settings()

st.set_page_config(
    page_title="PayPulse Stream",
    page_icon="💳",
    layout="wide",
)

st.title("PayPulse Stream")
st.caption("Stripe test payments processed through Kafka, Spark, and PostgreSQL.")

try:
    available_currencies = fetch_currencies(SETTINGS)
except Exception as exc:
    st.error("PostgreSQL is not reachable yet. Check the dashboard and database container logs.")
    st.exception(exc)
    st.stop()

if not available_currencies:
    st.info(
        "No payment events are stored yet. Run `stripe trigger payment_intent.succeeded` "
        "after the pipeline is running."
    )
    st.stop()

preferred_currency = SETTINGS.dashboard_default_currency.lower()
default_index = (
    available_currencies.index(preferred_currency)
    if preferred_currency in available_currencies
    else 0
)
selected_currency = st.selectbox(
    "Currency",
    options=available_currencies,
    index=default_index,
    format_func=str.upper,
)


# Purpose: selecting one currency avoids adding unrelated currencies together.


@st.fragment(run_every=f"{SETTINGS.dashboard_refresh_seconds}s")
def render_live_dashboard(currency: str) -> None:
    """Refresh analytics without rerunning the currency selector."""

    try:
        summary = fetch_summary(SETTINGS, currency)
        status_data = fetch_status_breakdown(SETTINGS, currency)
        hourly_data = fetch_hourly_activity(SETTINGS, currency)
        recent_data = fetch_recent_payments(SETTINGS, currency)
    except Exception as exc:
        st.error("The dashboard query failed. Check PostgreSQL and Spark logs.")
        st.exception(exc)
        return

    total_events = int(summary["total_events"] or 0)
    successful_events = int(summary["successful_events"] or 0)
    unsuccessful_events = int(summary["unsuccessful_events"] or 0)
    success_rate = (successful_events / total_events * 100) if total_events else 0

    metric_columns = st.columns(5)
    metric_columns[0].metric("Total events", f"{total_events:,}")
    metric_columns[1].metric("Successful", f"{successful_events:,}")
    metric_columns[2].metric("Other states", f"{unsuccessful_events:,}")
    metric_columns[3].metric("Success rate", f"{success_rate:.1f}%")
    metric_columns[4].metric(
        "Successful amount",
        format_money(summary["successful_amount"], currency),
    )

    chart_columns = st.columns(2)

    with chart_columns[0]:
        st.subheader("Events by payment status")
        if status_data.empty:
            st.info("No status data available.")
        else:
            st.bar_chart(
                status_data.set_index("payment_status")[["event_count"]],
                use_container_width=True,
            )

    with chart_columns[1]:
        st.subheader("Events during the last 24 hours")
        if hourly_data.empty:
            st.info("No events occurred during the last 24 hours.")
        else:
            hourly_chart = hourly_data.copy()
            hourly_chart["hour"] = pd.to_datetime(hourly_chart["hour"], utc=True)
            st.line_chart(
                hourly_chart.set_index("hour")[["event_count"]],
                use_container_width=True,
            )

    st.subheader("Recent payment events")
    if recent_data.empty:
        st.info("No recent events are available.")
    else:
        display_data = recent_data.copy()
        display_data["event_time"] = pd.to_datetime(display_data["event_time"], utc=True)
        display_data["amount"] = display_data["amount_major"].map(
            lambda value: format_money(value, currency)
        )
        display_data = display_data.drop(columns=["amount_major"])
        st.dataframe(
            display_data,
            use_container_width=True,
            hide_index=True,
            column_config={
                "event_time": st.column_config.DatetimeColumn("Event time", timezone="UTC"),
                "event_type": "Event type",
                "payment_id": "PaymentIntent",
                "payment_status": "Status",
                "amount": "Amount",
                "description": "Description",
                "is_successful": "Successful",
            },
        )

    st.caption(
        f"Automatically refreshed at {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )


# Purpose: the fragment refreshes live data while preserving the user's selected currency.


render_live_dashboard(selected_currency)
