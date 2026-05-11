from __future__ import annotations

import json
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import pandas as pd
import streamlit as st
from confluent_kafka import Consumer, KafkaError

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import settings


REFRESH_SECONDS: Final[float] = 0.5
MAX_EVENTS_PER_TICK: Final[int] = 300
MAX_STATE_CHANGES: Final[int] = 100

STATE_BADGE: Final[dict[str, str]] = {
    "WORKING": "WORKING",
    "MOVING": "MOVING",
    "IDLE": "IDLE",
}


@dataclass(frozen=True)
class DashboardState:
    consumer: Consumer
    group_id: str


def _fmt(seconds: float) -> str:
    total = int(seconds)
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _pct(part: float, total: float) -> str:
    return "-" if total <= 0 else f"{part / total * 100:.1f}%"


def _make_consumer() -> DashboardState:
    group_id = f"{settings.kafka_consumer_group_id}-{uuid.uuid4().hex[:8]}"
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.kafka_topic])
    return DashboardState(consumer=consumer, group_id=group_id)


def _init_session() -> None:
    defaults: dict[str, Any] = {
        "dashboard_state": None,
        "track_data": {},
        "event_log": [],
        "total_events": 0,
        "kafka_error": None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _get_dashboard_state() -> DashboardState:
    if st.session_state.dashboard_state is None:
        st.session_state.dashboard_state = _make_consumer()
    return st.session_state.dashboard_state


def _reset_consumer() -> None:
    state = st.session_state.get("dashboard_state")
    if state is not None:
        state.consumer.close()
    st.session_state.dashboard_state = _make_consumer()
    st.session_state.track_data = {}
    st.session_state.event_log = []
    st.session_state.total_events = 0
    st.session_state.kafka_error = None


def _consume_events(state: DashboardState) -> None:
    for _ in range(MAX_EVENTS_PER_TICK):
        msg = state.consumer.poll(0.01)
        if msg is None:
            break

        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                st.session_state.kafka_error = str(msg.error())
            continue

        try:
            event = json.loads(msg.value().decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            st.session_state.kafka_error = f"Invalid Kafka event: {exc}"
            continue

        track_id = int(event["track_id"])
        previous = st.session_state.track_data.get(track_id)
        st.session_state.track_data[track_id] = event
        st.session_state.total_events += 1

        if previous is None or previous.get("state") != event.get("state"):
            st.session_state.event_log.append(event)
            st.session_state.event_log = st.session_state.event_log[-MAX_STATE_CHANGES:]


def _render_preview() -> None:
    preview_path = settings.resolve_path(settings.preview_frame_path)
    if not settings.preview_enabled:
        st.info("Preview frame writing is disabled.")
        return

    if not preview_path.exists():
        st.info("Waiting for CV service preview frame...")
        return

    try:
        image_bytes = preview_path.read_bytes()
    except OSError:
        st.info("Preview frame is being updated...")
        return

    st.image(image_bytes, use_container_width=True)
    st.caption(f"Preview: {preview_path}")


def _render_metrics() -> None:
    track_data: dict[int, dict] = st.session_state.track_data
    working = sum(1 for event in track_data.values() if event.get("state") == "WORKING")
    moving = sum(1 for event in track_data.values() if event.get("state") == "MOVING")
    idle = sum(1 for event in track_data.values() if event.get("state") == "IDLE")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Tracked", len(track_data))
    col2.metric("Working", working)
    col3.metric("Moving", moving)
    col4.metric("Idle", idle)


def _render_tables() -> None:
    track_data: dict[int, dict] = st.session_state.track_data
    if not track_data:
        st.info("No Kafka events received yet. Start `python cv_service\\main.py`.")
        return

    rows = []
    for track_id, event in sorted(track_data.items()):
        working = float(event.get("working_seconds", 0.0))
        moving = float(event.get("moving_seconds", 0.0))
        idle = float(event.get("idle_seconds", 0.0))
        total = working + moving + idle
        rows.append(
            {
                "ID": track_id,
                "State": STATE_BADGE.get(event.get("state"), event.get("state")),
                "Motion": round(float(event.get("motion_score", 0.0)), 3),
                "Working": _fmt(working),
                "Moving": _fmt(moving),
                "Idle": _fmt(idle),
                "Util%": _pct(working, total),
                "Frame": event.get("frame_id"),
                "Video Time": _fmt(float(event.get("video_time_seconds", 0.0))),
            }
        )

    st.subheader("Equipment Status")
    st.dataframe(pd.DataFrame(rows), hide_index=True, width='stretch')

    chart_rows = []
    for track_id, event in sorted(track_data.items()):
        chart_rows.extend(
            [
                {"Track": f"#{track_id}", "State": "Working", "Seconds": event.get("working_seconds", 0.0)},
                {"Track": f"#{track_id}", "State": "Moving", "Seconds": event.get("moving_seconds", 0.0)},
                {"Track": f"#{track_id}", "State": "Idle", "Seconds": event.get("idle_seconds", 0.0)},
            ]
        )
    pivot = (
        pd.DataFrame(chart_rows)
        .pivot(index="Track", columns="State", values="Seconds")
        .fillna(0)
        .reindex(columns=["Working", "Moving", "Idle"], fill_value=0)
    )
    st.bar_chart(pivot, color=["#32cd32", "#ffa500", "#cc3333"])

    with st.expander("State Change Log", expanded=False):
        log_rows = []
        for event in reversed(st.session_state.event_log):
            log_rows.append(
                {
                    "Time": str(event.get("timestamp", ""))[11:19],
                    "Track": event.get("track_id"),
                    "State": event.get("state"),
                    "Motion": round(float(event.get("motion_score", 0.0)), 3),
                    "Frame": event.get("frame_id"),
                }
            )
        if log_rows:
            st.dataframe(pd.DataFrame(log_rows), hide_index=True, width='stretch')
        else:
            st.caption("No state transitions yet.")


@st.fragment(run_every=REFRESH_SECONDS)
def _live_dashboard() -> None:
    state = _get_dashboard_state()
    _consume_events(state)

    if st.session_state.kafka_error:
        st.error(st.session_state.kafka_error)

    st.caption(
        f"Kafka: `{settings.kafka_bootstrap_servers}` | "
        f"Topic: `{settings.kafka_topic}` | "
        f"Consumer group: `{state.group_id}` | "
        f"Events: `{st.session_state.total_events}`"
    )

    left, right = st.columns([3, 2], gap="medium")
    with left:
        st.subheader("Latest Annotated Frame")
        _render_preview()
    with right:
        st.subheader("Live Metrics")
        _render_metrics()

    st.divider()
    _render_tables()


def main() -> None:
    st.set_page_config(page_title="Excavator Kafka Monitor", layout="wide")
    _init_session()

    st.title("Excavator Activity Monitor")
    st.caption("Kafka-based dashboard. Run Docker Compose, then start the CV service.")

    with st.sidebar:
        st.header("Runtime")
        st.code("docker compose up -d\npython cv_service\\main.py", language="powershell")
        st.divider()
        if st.button("Reset Consumer"):
            _reset_consumer()
            st.rerun()

    _live_dashboard()


if __name__ == "__main__":
    main()
