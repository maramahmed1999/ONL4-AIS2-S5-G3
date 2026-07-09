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
from dashboard.components.system_monitoring import (
    render_alerts,
    render_confidence_trend_chart,
    render_hard_frames_note,
    render_kpi_cards,
)
from dashboard.components.tables import render_equipment_table, render_summary_table
from dashboard.components.video import render_live_preview
from dashboard.consumer import KafkaDashboardConsumer
from dashboard.persistence import PersistenceRepository
from dashboard.services.metrics import calculate_metrics
from dashboard.services.pipeline_manager import PipelineManager
from dashboard.store import EventStore

LIVE_REFRESH_SECONDS = 0.5
ANALYTICS_REFRESH_SECONDS = 1.5
STATUS_REFRESH_SECONDS = 1.0
SYSTEM_MONITORING_REFRESH_SECONDS = 5.0
SYSTEM_MONITORING_CHART_WINDOW_HOURS = 24.0
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
        session_id_provider=_pipeline_manager.current_session_id,
        persistence=_persistence,
    )
    consumer.start()
    return store, consumer

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


live_tab, analytics_tab, system_tab = st.tabs(["🎥 Live Monitor", "📊 Analytics", "🩺 System Monitoring"])

with live_tab:
    @st.fragment(run_every=LIVE_REFRESH_SECONDS)
    def render_live_tab() -> None:
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
        session_ids = persistence.list_sessions()
        current_session_id = pipeline_manager.current_session_id()

        if not session_ids:
            st.info("No sessions yet — upload a video or start a live camera to see analytics here.")
            return

        ALL_SESSIONS_OPTION = "__all__"
        options = [ALL_SESSIONS_OPTION, *session_ids]

        state_key = "analytics_selected_session_id"
        if st.session_state.get(state_key) not in options:
            st.session_state[state_key] = current_session_id if current_session_id in session_ids else session_ids[-1]

        def _session_option_label(sid: str) -> str:
            if sid == ALL_SESSIONS_OPTION:
                return "All sessions"
            return persistence.session_label(sid) or pipeline_manager.session_label(sid) or sid

        selected_session_id = st.selectbox(
            "Session",
            options=options,
            format_func=_session_option_label,
            key=state_key,
        )

        if selected_session_id == ALL_SESSIONS_OPTION:
            summaries = persistence.fetch_all_summaries()
            motion_history = persistence.fetch_all_motion_history()
            total_events = persistence.count_all_events()
        else:
            summaries = persistence.fetch_summary(selected_session_id)
            motion_history = persistence.fetch_motion_history(selected_session_id)
            total_events = persistence.count_events(selected_session_id)

        summary_column, download_column = st.columns((3, 1))
        with summary_column:
            render_summary_table(summaries)
        with download_column:
            st.write("")
            st.write("")
            render_download_button(summaries)

        chart_column_left, chart_column_right = st.columns(2)
        with chart_column_left:
        
            render_state_distribution_chart(summaries)
        with chart_column_right:
            render_utilization_chart(summaries)

        render_motion_chart(motion_history)

        st.caption(
            f"Received events: {total_events:,} · "
            f"Invalid events: {consumer.snapshot().invalid_messages:,}"
        )

    render_analytics_tab()

with system_tab:
    st.caption(
        "Live YOLO26n model performance, sampled every "
        f"{settings.model_metrics_interval_seconds:.0f}s while detection is running."
    )

    @st.fragment(run_every=SYSTEM_MONITORING_REFRESH_SECONDS)
    def render_system_kpis_and_alerts() -> None:
        latest_metric = persistence.latest_model_metric()

        render_alerts(
            latest_metric,
            low_confidence_threshold=settings.low_confidence_alert_percent,
            no_detections_minutes=settings.no_detections_alert_minutes,
        )
        render_kpi_cards(latest_metric)

        if latest_metric is None:
            st.info(
                "No model performance samples yet — start detection from the sidebar "
                "to see live YOLO26n metrics here."
            )

    render_system_kpis_and_alerts()

    st.divider()
    
    @st.fragment(run_every=60.0)
    def render_system_charts() -> None:
        samples = persistence.fetch_model_metrics_since(SYSTEM_MONITORING_CHART_WINDOW_HOURS)
        render_confidence_trend_chart(samples)

        render_hard_frames_note(
            settings.resolve_path(settings.hard_frames_dir),
            settings.hard_frame_conf_threshold,
        )

    render_system_charts()
