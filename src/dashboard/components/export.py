from __future__ import annotations

from datetime import datetime, timezone

import streamlit as st

from dashboard.models import TrackSummary
from dashboard.services.export import build_summary_workbook


def render_download_button(summaries: list[TrackSummary]) -> None:
    filename = f"excavator_summary_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.xlsx"
    workbook_bytes = build_summary_workbook(summaries)

    st.download_button(
        "⬇ Download Excel Summary",
        data=workbook_bytes,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        disabled=not summaries,
        use_container_width=True,
    )
