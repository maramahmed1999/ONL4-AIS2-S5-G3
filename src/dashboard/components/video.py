from __future__ import annotations

import time
from pathlib import Path

import streamlit as st

_STALE_AFTER_SECONDS = 5.0


def render_live_preview(preview_path: Path, pipeline_running: bool) -> None:
    st.subheader("Live Processed Video")

    if not preview_path.exists():
        st.info("No preview frame yet — start detection to see the live feed here.")
        return

    age_seconds = time.time() - preview_path.stat().st_mtime

    try:
        st.image(str(preview_path), use_container_width=True)
    except Exception:
        st.warning("Preview frame is being written — it will appear on the next refresh.")
        return

    if pipeline_running and age_seconds > _STALE_AFTER_SECONDS:
        st.warning(f"Feed looks stale ({age_seconds:.0f}s since last frame).", icon="🟠")
    elif not pipeline_running:
        st.caption("Showing the last frame from the most recent run.")
