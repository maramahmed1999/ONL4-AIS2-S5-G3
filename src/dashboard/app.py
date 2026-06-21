from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from config.settings import settings
from dashboard.components.charts import render_motion_chart
from dashboard.components.overview import render_metric_cards
from dashboard.components.status import render_connection_status
from dashboard.components.tables import render_equipment_table, render_transition_table
from dashboard.consumer import KafkaDashboardConsumer
from dashboard.services.metrics import calculate_metrics
from dashboard.store import EventStore

REFRESH_INTERVAL_SECONDS = 0.5
OFFLINE_AFTER_SECONDS = 5.0

st.set_page_config(
    page_title="Excavator Activity Monitor",
    page_icon="🏗️",
    layout="wide",
)


@st.cache_resource(show_spinner=False)
def get_dashboard_runtime() -> tuple[EventStore, KafkaDashboardConsumer]:
    store = EventStore(
        max_events=1000,
        max_transitions=250,
        stale_track_seconds=10.0,
    )
    consumer = KafkaDashboardConsumer(
        store=store,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic,
        group_id=settings.kafka_consumer_group_id,
    )
    consumer.start()
    return store, consumer


store, consumer = get_dashboard_runtime()

st.title("Excavator Activity Monitor")
st.caption("Realtime Kafka telemetry from the computer-vision pipeline")


@st.fragment(run_every=REFRESH_INTERVAL_SECONDS)
def render_live_dashboard() -> None:
    snapshot = store.snapshot()
    consumer_status = consumer.snapshot()
    metrics = calculate_metrics(snapshot)

    render_connection_status(
        consumer_status,
        metrics,
        offline_after_seconds=OFFLINE_AFTER_SECONDS,
    )
    render_metric_cards(metrics)

    equipment_column, transitions_column = st.columns((1.3, 1))
    with equipment_column:
        render_equipment_table(snapshot.latest_by_track)
    with transitions_column:
        render_transition_table(snapshot.transitions)

    render_motion_chart(snapshot.history)

    st.caption(
        f"Received events: {snapshot.total_events:,} · "
        f"Invalid events: {consumer_status.invalid_messages:,}"
    )


render_live_dashboard()
