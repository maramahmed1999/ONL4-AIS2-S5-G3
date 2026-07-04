from __future__ import annotations

import pandas as pd
import streamlit as st

from dashboard.models import DashboardEvent, TrackSummary


def render_motion_chart(history: tuple[DashboardEvent, ...]) -> None:
    st.subheader("Motion score")
    if not history:
        st.info("Motion history will appear after events arrive.")
        return

    frame = pd.DataFrame(
        {
            "timestamp": [event.timestamp for event in history],
            "track": [f"Track {event.track_id}" for event in history],
            "motion_score": [event.motion_score for event in history],
        }
    )
    chart_data = frame.pivot_table(
        index="timestamp",
        columns="track",
        values="motion_score",
        aggfunc="last",
    ).sort_index()
    st.line_chart(chart_data, height=280)


def render_state_distribution_chart(latest_by_track: dict[int, DashboardEvent]) -> None:
    """Working vs Idle equipment counts, right now."""
    st.subheader("Working vs Idle")
    if not latest_by_track:
        st.info("State distribution will appear once equipment is tracked.")
        return

    counts = pd.Series([event.state for event in latest_by_track.values()]).value_counts()
    chart_data = pd.DataFrame(
        {"count": [int(counts.get("WORKING", 0)), int(counts.get("IDLE", 0))]},
        index=["WORKING", "IDLE"],
    )
    st.bar_chart(chart_data, height=240)


def render_utilization_chart(summaries: list[TrackSummary]) -> None:
    """Utilization percent per excavator, across the whole session."""
    st.subheader("Utilization by excavator")
    if not summaries:
        st.info("Utilization will appear once excavators have been tracked.")
        return

    chart_data = pd.DataFrame(
        {"Utilization %": [summary.utilization_percent for summary in summaries]},
        index=[f"Track {summary.track_id}" for summary in summaries],
    )
    st.bar_chart(chart_data, height=280)
