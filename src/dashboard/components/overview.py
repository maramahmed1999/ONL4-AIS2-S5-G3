from __future__ import annotations

import streamlit as st

from dashboard.services.metrics import DashboardMetrics


def render_metric_cards(metrics: DashboardMetrics) -> None:
    columns = st.columns(5)
    columns[0].metric("Tracked", metrics.tracked_equipment)
    columns[1].metric("Working", metrics.working_equipment)
    columns[2].metric("Idle", metrics.idle_equipment)
    columns[3].metric("Utilization", f"{metrics.utilization_percent:.1f}%")
    columns[4].metric("Total working", _format_duration(metrics.working_seconds))


def _format_duration(seconds: float) -> str:
    total_seconds = max(0, int(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"
