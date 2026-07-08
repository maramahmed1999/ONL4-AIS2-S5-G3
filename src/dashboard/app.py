from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from config.settings import settings
from dashboard.components.charts import (
    render_motion_chart,
    render_state_distribution_chart,
    render_utilization_chart,
)
from dashboard.components.controls import render_source_and_actions, render_status_panel
from dashboard.components.export import render_download_button
from dashboard.components.overview import render_metric_cards
from dashboard.components.status import render_connection_status
from dashboard.components.tables import render_equipment_table, render_summary_table
from dashboard.components.video import render_live_preview
from dashboard.consumer import KafkaDashboardConsumer
from dashboard.persistence import PersistenceRepository
from dashboard.services.metrics import build_all_track_summaries, build_track_summaries, calculate_metrics
from dashboard.services.pipeline_manager import PipelineManager
from dashboard.store import EventStore

LIVE_REFRESH_SECONDS = 0.5
ANALYTICS_REFRESH_SECONDS = 1.5
STATUS_REFRESH_SECONDS = 1.0
OFFLINE_AFTER_SECONDS = 5.0

st.set_page_config(
    page_title="Excavator Activity Monitor",
    page_icon="🏗️",
    layout="wide",
)


@st.cache_resource(show_spinner=False)
def get_persistence_repository() -> PersistenceRepository:
    return PersistenceRepository(db_path=settings.resolve_path("runtime/dashboard.sqlite3"))


@st.cache_resource(show_spinner=False)
def get_pipeline_manager(_persistence: PersistenceRepository) -> PipelineManager:
    return PipelineManager(
        src_root=SRC_ROOT,
        uploads_dir=settings.resolve_path("runtime/uploads"),
        log_path=settings.resolve_path("runtime/pipeline.log"),
        persistence=_persistence,
    )


@st.cache_resource(show_spinner=False)
def get_dashboard_runtime(
    _pipeline_manager: PipelineManager,
    _persistence: PersistenceRepository,
) -> tuple[EventStore, KafkaDashboardConsumer]:
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
        # Every event gets tagged with whatever session is currently
        # running, so it's never merged with a different upload/stream.
        session_id_provider=_pipeline_manager.current_session_id,
        persistence=_persistence,
    )
    consumer.start()
    return store, consumer


# pipeline_manager has to exist before get_dashboard_runtime() is called,
# since the consumer needs pipeline_manager.current_session_id as its
# session provider.
persistence = get_persistence_repository()
pipeline_manager = get_pipeline_manager(persistence)
store, consumer = get_dashboard_runtime(pipeline_manager, persistence)
preview_path = settings.resolve_path(settings.preview_frame_path)

st.title("Excavator Activity Monitor")
st.caption("Upload a video or connect a live camera, then watch detection, tracking, and activity state in real time.")

with st.sidebar:
    render_source_and_actions(pipeline_manager)
    st.divider()

    @st.fragment(run_every=STATUS_REFRESH_SECONDS)
    def render_sidebar_status() -> None:
        render_status_panel(pipeline_manager.status())
        with st.expander("Pipeline log"):
            log_text = pipeline_manager.tail_log(max_lines=200)
            st.code(log_text or "No log output yet.", language="text")

    render_sidebar_status()


live_tab, analytics_tab = st.tabs(["🎥 Live Monitor", "📊 Analytics"])

with live_tab:
    @st.fragment(run_every=LIVE_REFRESH_SECONDS)
    def render_live_tab() -> None:
        # Live tab always tracks whatever session is currently running,
        # regardless of what's selected in the Analytics tab picker below.
        snapshot = store.snapshot(pipeline_manager.current_session_id())
        consumer_status = consumer.snapshot()
        metrics = calculate_metrics(snapshot)

        render_connection_status(
            consumer_status,
            metrics,
            offline_after_seconds=OFFLINE_AFTER_SECONDS,
        )
        render_metric_cards(metrics)

        video_column, equipment_column = st.columns((1.4, 1))
        with video_column:
            render_live_preview(preview_path, pipeline_manager.is_running())
        with equipment_column:
            render_equipment_table(snapshot.latest_by_track)

    render_live_tab()

with analytics_tab:
    @st.fragment(run_every=ANALYTICS_REFRESH_SECONDS)
    def render_analytics_tab() -> None:
        # Session picker lives inside the fragment so it stays in sync as
        # new sessions/events arrive — it used to sit outside the fragment
        # and only refreshed on a full page rerun, which made it look stuck
        # on "No sessions yet" even after data started flowing.
        session_ids = store.list_sessions()
        current_session_id = pipeline_manager.current_session_id()

        if not session_ids:
            st.info("No sessions yet — upload a video or start a live camera to see analytics here.")
            return

        # "All" is a synthetic option prepended to the real session IDs —
        # picking it merges every session's tracking into one combined view.
        ALL_SESSIONS_OPTION = "__all__"
        options = [ALL_SESSIONS_OPTION, *session_ids]

        # Preserve whatever the user manually picked across refreshes;
        # only fall back to the current/most-recent session the first time,
        # or if their pick is no longer valid.
        state_key = "analytics_selected_session_id"
        if st.session_state.get(state_key) not in options:
            st.session_state[state_key] = current_session_id if current_session_id in session_ids else session_ids[-1]

        def _session_option_label(sid: str) -> str:
            if sid == ALL_SESSIONS_OPTION:
                return "All sessions"
            return pipeline_manager.session_label(sid) or sid

        selected_session_id = st.selectbox(
            "Session",
            options=options,
            format_func=_session_option_label,
            key=state_key,
        )

        if selected_session_id == ALL_SESSIONS_OPTION:
            all_snapshot = store.snapshot_all()
            summaries = build_all_track_summaries(all_snapshot)
            state_events = all_snapshot.latest_events
            motion_history = all_snapshot.history
            total_events = all_snapshot.total_events
        else:
            snapshot = store.snapshot(selected_session_id)
            summaries = build_track_summaries(snapshot)
            state_events = snapshot.latest_by_track.values()
            motion_history = [
                (snapshot.session_id or "unknown", event) for event in snapshot.history
            ]
            total_events = snapshot.total_events

        summary_column, download_column = st.columns((3, 1))
        with summary_column:
            render_summary_table(summaries)
        with download_column:
            st.write("")
            st.write("")
            render_download_button(summaries)

        chart_column_left, chart_column_right = st.columns(2)
        with chart_column_left:
            render_state_distribution_chart(state_events)
        with chart_column_right:
            render_utilization_chart(summaries)

        render_motion_chart(motion_history)

        st.caption(
            f"Received events: {total_events:,} · "
            f"Invalid events: {consumer.snapshot().invalid_messages:,}"
        )

    render_analytics_tab()
