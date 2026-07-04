from __future__ import annotations

import streamlit as st

from dashboard.models import PipelineSource, PipelineStatus
from dashboard.services.pipeline_manager import PipelineManager
from zoneinfo import ZoneInfo

CAIRO = ZoneInfo("Africa/Cairo")

_UPLOAD_TYPES = ["mp4", "avi", "mov", "mkv", "m4v"]


def render_source_and_actions(manager: PipelineManager) -> None:
    """Sidebar: choose Upload Video / Live Camera, then Start/Stop detection."""
    st.subheader("Video Source")

    source_choice = st.radio(
        "Source",
        options=[PipelineSource.UPLOAD, PipelineSource.CAMERA],
        format_func=lambda source: "Upload Video" if source is PipelineSource.UPLOAD else "Live Camera",
        label_visibility="collapsed",
        key="source_choice",
    )

    uploaded_file = None
    camera_index = 0

    if source_choice is PipelineSource.UPLOAD:
        uploaded_file = st.file_uploader(
            "Video file",
            type=_UPLOAD_TYPES,
            help="Long videos are streamed to disk and processed frame-by-frame — the whole file is never held in memory at once.",
        )
    else:
        camera_index = st.number_input(
            "Camera device index",
            min_value=0,
            max_value=16,
            value=0,
            step=1,
            help="Index of the camera device on the machine running the pipeline (usually 0 for the first/default webcam).",
        )

    is_running = manager.is_running()
    start_col, stop_col = st.columns(2)

    with start_col:
        start_clicked = st.button(
            "▶ Start Detection",
            use_container_width=True,
            type="primary",
            disabled=is_running,
        )
    with stop_col:
        stop_clicked = st.button(
            "■ Stop Detection",
            use_container_width=True,
            disabled=not is_running,
        )

    if start_clicked:
        _handle_start(manager, source_choice, uploaded_file, int(camera_index))

    if stop_clicked:
        with st.spinner("Stopping detection…"):
            manager.stop()
        st.rerun()


def _handle_start(
    manager: PipelineManager,
    source_choice: PipelineSource,
    uploaded_file,
    camera_index: int,
) -> None:
    try:
        if source_choice is PipelineSource.UPLOAD:
            if uploaded_file is None:
                st.warning("Choose a video file first.")
                return
            with st.spinner("Saving upload…"):
                video_path = manager.save_upload(uploaded_file, uploaded_file.name)
            manager.start(source_choice, video_path=video_path, camera_index=None)
        else:
            manager.start(source_choice, video_path=None, camera_index=camera_index)
        st.rerun()
    except (RuntimeError, ValueError) as exc:
        st.error(str(exc))


def render_status_panel(status: PipelineStatus) -> None:
    """Live status badge + collapsible log tail. Meant to run inside an
    auto-refreshing fragment so it reflects the pipeline finishing/crashing
    even without user interaction."""
    st.subheader("Pipeline Status")

    if status.running:
        st.success(f"Running · {status.source_label}", icon="🟢")
        if status.started_at is not None:
            st.caption(f"Started {status.started_at.astimezone(CAIRO).strftime('%H:%M:%S')}")
    elif status.error:
        st.error(status.error, icon="⚠️")
    elif status.return_code is not None:
        st.info(f"Finished (exit code {status.return_code}).", icon="⏹️")
    else:
        st.info("Idle — choose a source and start detection.", icon="⚪")
