from __future__ import annotations

from datetime import datetime, timezone
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
