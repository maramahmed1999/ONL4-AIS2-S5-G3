from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from dashboard.store import DashboardSnapshot


@dataclass(frozen=True)
class DashboardMetrics:
    tracked_equipment: int
    working_equipment: int
    idle_equipment: int
    working_seconds: float
    idle_seconds: float
    utilization_percent: float
    event_lag_seconds: float | None


def calculate_metrics(
    snapshot: DashboardSnapshot,
    now: datetime | None = None,
) -> DashboardMetrics:
    latest = tuple(snapshot.latest_by_track.values())
    working_seconds = sum(event.working_seconds for event in latest)
    idle_seconds = sum(event.idle_seconds for event in latest)
    total_seconds = working_seconds + idle_seconds
    utilization = working_seconds / total_seconds * 100.0 if total_seconds > 0 else 0.0

    lag = None
    if snapshot.last_received_at is not None:
        current_time = now or datetime.now(timezone.utc)
        lag = max(0.0, (current_time - snapshot.last_received_at).total_seconds())

    return DashboardMetrics(
        tracked_equipment=len(latest),
        working_equipment=sum(event.state == "WORKING" for event in latest),
        idle_equipment=sum(event.state == "IDLE" for event in latest),
        working_seconds=working_seconds,
        idle_seconds=idle_seconds,
        utilization_percent=utilization,
        event_lag_seconds=lag,
    )
