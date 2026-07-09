from __future__ import annotations

from typing import Iterable, Sequence

import pandas as pd
import streamlit as st

from dashboard.models import DashboardEvent, TrackSummary


def render_motion_chart(history: Sequence[tuple[str, DashboardEvent]]) -> None:
    
    st.subheader("Motion score")
    if not history:
        st.info("Motion history will appear after events arrive.")
        return

    multi_session = len({session_id for session_id, _event in history}) > 1

    def _label(session_id: str, event: DashboardEvent) -> str:
        if multi_session:
            return f"{session_id} · Track {event.track_id}"
        return f"Track {event.track_id}"

    frame = pd.DataFrame(
        {
            "timestamp": [event.timestamp for _session_id, event in history],
            "track": [_label(session_id, event) for session_id, event in history],
            "motion_score": [event.motion_score for _session_id, event in history],
        }
    )
    chart_data = frame.pivot_table(
        index="timestamp",
        columns="track",
        values="motion_score",
        aggfunc="last",
    ).sort_index()
    st.line_chart(chart_data, height=280)


def render_state_distribution_chart(events: Iterable[DashboardEvent]) -> None:
    st.subheader("Working vs Idle")
    events = list(events)
    if not events:
        st.info("State distribution will appear once equipment is tracked.")
        return

    counts = pd.Series([event.state for event in events]).value_counts()
    chart_data = pd.DataFrame(
        {"count": [int(counts.get("WORKING", 0)), int(counts.get("IDLE", 0))]},
        index=["WORKING", "IDLE"],
    )
    st.bar_chart(chart_data, height=240)


def render_utilization_chart(summaries: list[TrackSummary]) -> None:
    st.subheader("Utilization by excavator")
    if not summaries:
        st.info("Utilization will appear once excavators have been tracked.")
        return
    multi_session = len({summary.session_id for summary in summaries}) > 1
    labels = [
        f"{summary.session_id} · Track {summary.track_id}" if multi_session else f"Track {summary.track_id}"
        for summary in summaries
    ]

    chart_data = pd.DataFrame(
        {"Utilization %": [summary.utilization_percent for summary in summaries]},
        index=labels,
    )
    st.bar_chart(chart_data, height=280)
