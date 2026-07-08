from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DashboardEvent(BaseModel):
    """Validated event contract shared by Kafka and the dashboard."""

    model_config = ConfigDict(frozen=True)

    track_id: int = Field(ge=0)
    timestamp: datetime
    frame_id: int = Field(ge=0)
    video_time_seconds: float = Field(ge=0)
    state: Literal["IDLE", "WORKING"]
    motion_score: float
    bbox: tuple[int, int, int, int]
    working_seconds: float = Field(ge=0)
    idle_seconds: float = Field(ge=0)

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @property
    def utilization_percent(self) -> float:
        total = self.working_seconds + self.idle_seconds
        return (self.working_seconds / total * 100.0) if total > 0 else 0.0


class TrackSummary(BaseModel):
    """One row of the per-excavator summary table / Excel export.

    Total observed time and utilization are derived directly from the
    state machine's own bookkeeping (working_seconds + idle_seconds), which
    is the single source of truth already published by the CV pipeline —
    this avoids maintaining a second, potentially-drifting clock in the
    dashboard.
    """

    model_config = ConfigDict(frozen=True)

    track_id: int
    session_id: str
    state: Literal["IDLE", "WORKING"]
    working_seconds: float = Field(ge=0)
    idle_seconds: float = Field(ge=0)
    last_seen: datetime

    @property
    def total_observed_seconds(self) -> float:
        return self.working_seconds + self.idle_seconds

    @property
    def utilization_percent(self) -> float:
        total = self.total_observed_seconds
        return (self.working_seconds / total * 100.0) if total > 0 else 0.0

    @classmethod
    def from_event(cls, event: DashboardEvent, session_id: str) -> "TrackSummary":
        return cls(
            track_id=event.track_id,
            session_id=session_id,
            state=event.state,
            working_seconds=event.working_seconds,
            idle_seconds=event.idle_seconds,
            last_seen=event.timestamp,
        )


class PipelineSource(str, Enum):
    """Which kind of video source the dashboard should launch the CV pipeline against."""

    UPLOAD = "upload"
    CAMERA = "camera"


class PipelineStatus(BaseModel):
    """Point-in-time status of the managed cv_service subprocess."""

    model_config = ConfigDict(frozen=True)

    running: bool
    pid: int | None = None
    source_label: str | None = None
    started_at: datetime | None = None
    return_code: int | None = None
    error: str | None = None
    session_id: str | None = None
