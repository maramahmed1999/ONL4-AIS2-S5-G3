from __future__ import annotations

from io import BytesIO

import pandas as pd

from dashboard.models import TrackSummary
from zoneinfo import ZoneInfo
CAIRO = ZoneInfo("Africa/Cairo")

_COLUMNS = [
    "Session ID",
    "Track ID",
    "Last state",
    "Working Time (s)",
    "Idle Time (s)",
    "Total Observed Time (s)",
    "Utilization (%)",
    "Date",
    "Last Seen Time",
]


def build_summary_dataframe(summaries: list[TrackSummary]) -> pd.DataFrame:
    """Same rows/columns used by the live summary table and the Excel export,
    so the download always matches what's on screen."""
    rows = [
        {
            "Session ID": summary.session_id,
            "Track ID": summary.track_id,
            "Last state": summary.state,
            "Working Time (s)": round(summary.working_seconds, 2),
            "Idle Time (s)": round(summary.idle_seconds, 2),
            "Total Observed Time (s)": round(summary.total_observed_seconds, 2),
            "Utilization (%)": round(summary.utilization_percent, 2),
            "Date": summary.last_seen.astimezone(CAIRO).strftime("%Y-%m-%d"),
            "Last Seen Time": summary.last_seen.astimezone(CAIRO).strftime("%H:%M:%S"),
        }
        for summary in summaries
    ]
    return pd.DataFrame(rows, columns=_COLUMNS)


def build_summary_workbook(summaries: list[TrackSummary]) -> bytes:
    """Render the excavator summary as an .xlsx file and return its bytes."""
    dataframe = build_summary_dataframe(summaries)

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        sheet_name = "Excavator Summary"
        dataframe.to_excel(writer, index=False, sheet_name=sheet_name)

        worksheet = writer.sheets[sheet_name]
        for column_index, column_name in enumerate(_COLUMNS, start=1):
            max_content_width = (
                dataframe[column_name].astype(str).map(len).max()
                if not dataframe.empty
                else 0
            )
            width = max(len(column_name), int(max_content_width)) + 2
            worksheet.column_dimensions[worksheet.cell(row=1, column=column_index).column_letter].width = width

    return buffer.getvalue()
