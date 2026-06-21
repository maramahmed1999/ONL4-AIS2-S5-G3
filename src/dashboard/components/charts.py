from __future__ import annotations

import pandas as pd
import streamlit as st

from dashboard.models import DashboardEvent


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
