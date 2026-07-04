from __future__ import annotations

import pandas as pd
import streamlit as st

from dashboard.models import DashboardEvent, TrackSummary
from dashboard.services.export import build_summary_dataframe
from zoneinfo import ZoneInfo
CAIRO = ZoneInfo("Africa/Cairo")


def render_equipment_table(latest_by_track: dict[int, DashboardEvent]) -> None:
    st.subheader("Current equipment")
    if not latest_by_track:
        st.info("No tracked equipment yet.")
        return

    rows = [_event_row(event) for _, event in sorted(latest_by_track.items())]
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def render_transition_table(transitions: tuple[DashboardEvent, ...]) -> None:
    st.subheader("Recent state changes")
    if not transitions:
        st.info("State changes will appear here.")
        return

    rows = [_event_row(event) for event in reversed(transitions[-20:])]
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def render_summary_table(summaries: list[TrackSummary]) -> None:
    """One row per excavator: Track ID, Working Time, Idle Time, Utilization.

    Sourced from the same data used by the Excel export, so what's on screen
    always matches what gets downloaded.
    """
    st.subheader("Excavator Summary")
    if not summaries:
        st.info("Summary will appear once excavators have been tracked.")
        return

    st.dataframe(
        build_summary_dataframe(summaries),
        use_container_width=True,
        hide_index=True,
    )


def _event_row(event: DashboardEvent) -> dict[str, object]:
    return {
        "Track": event.track_id,
        "State": event.state,
        "Video time": f"{event.video_time_seconds:.1f}s",
        "Working": f"{event.working_seconds:.1f}s",
        "Idle": f"{event.idle_seconds:.1f}s",
        "Utilization": f"{event.utilization_percent:.1f}%",
        "Motion": round(event.motion_score, 3),
        "Event time": event.timestamp.astimezone(CAIRO).strftime("%H:%M:%S"),
    }
