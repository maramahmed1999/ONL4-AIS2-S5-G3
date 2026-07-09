from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import pandas as pd
import streamlit as st

from dashboard.persistence import ModelMetricSample


def _fmt_percent(value: float | None) -> str:
    return f"{value:.1f}%" if value is not None else "—"


def _fmt_number(value: float | None, suffix: str = "") -> str:
    return f"{value:.1f}{suffix}" if value is not None else "—"


def render_kpi_cards(latest: ModelMetricSample | None) -> None:
  
    columns = st.columns(3)
    columns[0].metric(
        "Average Confidence",
        _fmt_percent(latest.avg_confidence * 100 if latest and latest.avg_confidence is not None else None),
    )
    columns[1].metric("Current FPS", _fmt_number(latest.fps if latest else None))
    columns[2].metric("Avg Inference Time", _fmt_number(latest.inference_time_ms if latest else None, " ms"))


def render_alerts(
    latest: ModelMetricSample | None,
    low_confidence_threshold: float,
    no_detections_minutes: float,
) -> None:
   
    alerts: list[str] = []

    if latest is not None and latest.avg_confidence is not None:
        avg_confidence_pct = latest.avg_confidence * 100
        if avg_confidence_pct < low_confidence_threshold:
            alerts.append(
                f"⚠️ **Low average confidence** — {avg_confidence_pct:.1f}% "
                f"(below the {low_confidence_threshold:.0f}% threshold)."
            )

    if latest is not None:
        age_minutes = (datetime.now(timezone.utc) - latest.timestamp.astimezone(timezone.utc)).total_seconds() / 60.0
        if age_minutes > no_detections_minutes:
            alerts.append(
                f"🚫 **No detections received** for {age_minutes:.0f} minutes "
            )

    for message in alerts:
        st.warning(message, icon="⚠️")


def render_confidence_trend_chart(samples: Sequence[ModelMetricSample]) -> None:
    st.subheader("Average Confidence Trend (last 24h)")
    usable = [s for s in samples if s.avg_confidence is not None]
    if not usable:
        st.info("Confidence trend will appear once the model starts producing detections.")
        return

    frame = pd.DataFrame(
        {
            "Time": [s.timestamp for s in usable],
            "Average Confidence (%)": [s.avg_confidence * 100 for s in usable],
        }
    ).set_index("Time")
    st.line_chart(frame, height=280)


def render_hard_frames_note(hard_frames_dir: Path, conf_threshold: float) -> None:
    try:
        count = sum(1 for _ in hard_frames_dir.glob("*.jpg"))
    except OSError:
        count = None

    if count is None:
        return
    st.caption(
        f"🗂️ {count:,} low-confidence frame(s) (< {conf_threshold:.0%} confidence) "
        f"saved to `{hard_frames_dir}` for review."
    )
