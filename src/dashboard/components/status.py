from __future__ import annotations

import streamlit as st

from dashboard.consumer import ConsumerSnapshot
from dashboard.services.metrics import DashboardMetrics


def render_connection_status(
    consumer: ConsumerSnapshot,
    metrics: DashboardMetrics,
    offline_after_seconds: float,
) -> None:
    if consumer.last_error:
        st.error(f"Kafka issue: {consumer.last_error}", icon="⚠️")
        return

    if not consumer.running:
        st.error("Kafka consumer is stopped.", icon="🔴")
        return

    if metrics.event_lag_seconds is None:
        st.info("Connected to Kafka and waiting for the first event.", icon="🔵")
    elif metrics.event_lag_seconds > offline_after_seconds:
        st.warning(
            f"No new event for {metrics.event_lag_seconds:.1f} seconds.",
            icon="🟠",
        )
    else:
        st.success(
            f"Live · latest event {metrics.event_lag_seconds:.1f}s ago",
            icon="🟢",
        )
