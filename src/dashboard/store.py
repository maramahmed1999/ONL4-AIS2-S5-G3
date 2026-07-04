from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock

from dashboard.models import DashboardEvent


@dataclass(frozen=True)
class DashboardSnapshot:
    latest_by_track: dict[int, DashboardEvent]
    history: tuple[DashboardEvent, ...]
    transitions: tuple[DashboardEvent, ...]
    last_received_at: datetime | None
    total_events: int
    # Latest event per track_id, never pruned by staleness. Used for the
    # cumulative excavator summary table / Excel export so a machine that
    # finishes working and leaves frame doesn't vanish from the report.
    # Defaults to an empty dict so existing callers that construct
    # DashboardSnapshot without this field keep working unchanged.
    all_latest_by_track: dict[int, DashboardEvent] = field(default_factory=dict)


class EventStore:
    """Thread-safe, bounded in-memory read model for the dashboard."""

    def __init__(
        self,
        max_events: int = 1000,
        max_transitions: int = 250,
        stale_track_seconds: float = 10.0,
    ) -> None:
        if max_events < 1 or max_transitions < 1 or stale_track_seconds <= 0:
            raise ValueError("Store limits and stale timeout must be positive")

        self._lock = RLock()
        self._history: deque[DashboardEvent] = deque(maxlen=max_events)
        self._transitions: deque[DashboardEvent] = deque(maxlen=max_transitions)
        self._latest_by_track: dict[int, DashboardEvent] = {}
        self._all_latest_by_track: dict[int, DashboardEvent] = {}
        self._received_by_track: dict[int, datetime] = {}
        self._last_received_at: datetime | None = None
        self._total_events = 0
        self._stale_track_seconds = stale_track_seconds

    def append(
        self,
        event: DashboardEvent,
        received_at: datetime | None = None,
    ) -> None:
        received_at = received_at or datetime.now(timezone.utc)
        previous = None

        with self._lock:
            previous = self._latest_by_track.get(event.track_id)
            self._latest_by_track[event.track_id] = event
            self._all_latest_by_track[event.track_id] = event
            self._received_by_track[event.track_id] = received_at
            self._history.append(event)
            self._last_received_at = received_at
            self._total_events += 1

            if previous is None or previous.state != event.state:
                self._transitions.append(event)

    def snapshot(self, now: datetime | None = None) -> DashboardSnapshot:
        current_time = now or datetime.now(timezone.utc)
        with self._lock:
            active_tracks = {
                track_id: event
                for track_id, event in self._latest_by_track.items()
                if (
                    current_time - self._received_by_track[track_id]
                ).total_seconds() <= self._stale_track_seconds
            }
            return DashboardSnapshot(
                latest_by_track=active_tracks,
                history=tuple(self._history),
                transitions=tuple(self._transitions),
                last_received_at=self._last_received_at,
                total_events=self._total_events,
                all_latest_by_track=dict(self._all_latest_by_track),
            )
