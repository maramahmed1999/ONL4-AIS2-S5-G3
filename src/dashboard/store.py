from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock

from dashboard.models import DashboardEvent

# Sentinel session used when an event arrives with no known active session
# (e.g. a leftover Kafka message right as a run is starting/stopping), so it
# doesn't get silently merged into whichever session happens to be current.
_UNKNOWN_SESSION = "unknown"


@dataclass(frozen=True)
class DashboardSnapshot:
    # Which session this snapshot is for. None only when no session was
    # requested / no sessions exist yet.
    session_id: str | None
    latest_by_track: dict[int, DashboardEvent]
    history: tuple[DashboardEvent, ...]
    transitions: tuple[DashboardEvent, ...]
    last_received_at: datetime | None
    total_events: int
    # Latest event per track_id, never pruned by staleness. Used for the
    # cumulative excavator summary table / Excel export so a machine that
    # finishes working and leaves frame doesn't vanish from the report.
    all_latest_by_track: dict[int, DashboardEvent] = field(default_factory=dict)


@dataclass(frozen=True)
class AllSessionsSnapshot:
    """Read-model merged across every session combined (the Analytics tab's
    "All" option).

    Unlike DashboardSnapshot, every per-track entry here keeps its
    session_id alongside the event instead of being collapsed into a plain
    track_id-keyed dict. That's deliberate: two different sessions can both
    have a "Track 1", and collapsing them onto the same int key would
    silently merge or overwrite one session's excavator with another's.
    Anything that needs to distinguish tracks (the summary table, Excel
    export, utilization chart, motion chart) is built from the
    (session_id, event) pairs below; latest_events is only ever used for
    plain aggregate counts (e.g. the working/idle split), where collisions
    don't matter because nothing is keyed by track_id.
    """

    session_ids: tuple[str, ...]
    latest_events: tuple[DashboardEvent, ...]
    all_latest_events: tuple[tuple[str, DashboardEvent], ...]
    history: tuple[tuple[str, DashboardEvent], ...]
    last_received_at: datetime | None
    total_events: int


class EventStore:
    """Thread-safe, bounded in-memory read model for the dashboard.

    Every excavator "track" is scoped to the session (one video upload or
    one live-camera run) it was observed in, via a composite
    (session_id, track_id) key. This is what stops Track ID 1 from a second
    upload/stream from overwriting Track ID 1 from the first one — they are
    different keys, so both are kept and can be viewed independently.
    """

    def __init__(
        self,
        max_events: int = 1000,
        max_transitions: int = 250,
        stale_track_seconds: float = 10.0,
    ) -> None:
        if max_events < 1 or max_transitions < 1 or stale_track_seconds <= 0:
            raise ValueError("Store limits and stale timeout must be positive")

        self._lock = RLock()
        self._max_events = max_events
        self._max_transitions = max_transitions
        self._stale_track_seconds = stale_track_seconds

        # Per-session bounded history/transitions, so one session's volume of
        # events can never crowd another session's events out of the deque.
        self._history: dict[str, deque[DashboardEvent]] = {}
        self._transitions: dict[str, deque[DashboardEvent]] = {}

        # Composite (session_id, track_id) keys — the core of the fix.
        self._latest_by_track: dict[tuple[str, int], DashboardEvent] = {}
        self._all_latest_by_track: dict[tuple[str, int], DashboardEvent] = {}
        self._received_by_track: dict[tuple[str, int], datetime] = {}

        self._last_received_at: dict[str, datetime | None] = {}
        self._total_events: dict[str, int] = {}

        # Chronological list of session IDs first-seen, oldest to newest —
        # backs the Analytics tab's session picker.
        self._session_order: list[str] = []

    def append(
        self,
        event: DashboardEvent,
        session_id: str | None = None,
        received_at: datetime | None = None,
    ) -> None:
        received_at = received_at or datetime.now(timezone.utc)
        session_id = session_id or _UNKNOWN_SESSION
        key = (session_id, event.track_id)

        with self._lock:
            if session_id not in self._session_order:
                self._session_order.append(session_id)
                self._history[session_id] = deque(maxlen=self._max_events)
                self._transitions[session_id] = deque(maxlen=self._max_transitions)
                self._total_events[session_id] = 0

            previous = self._latest_by_track.get(key)
            self._latest_by_track[key] = event
            self._all_latest_by_track[key] = event
            self._received_by_track[key] = received_at
            self._history[session_id].append(event)
            self._last_received_at[session_id] = received_at
            self._total_events[session_id] += 1

            if previous is None or previous.state != event.state:
                self._transitions[session_id].append(event)

    def snapshot(self, session_id: str | None, now: datetime | None = None) -> DashboardSnapshot:
        """Read-model for exactly one session. Unknown/None session -> empty snapshot."""
        current_time = now or datetime.now(timezone.utc)
        with self._lock:
            if session_id is None or session_id not in self._session_order:
                return DashboardSnapshot(
                    session_id=session_id,
                    latest_by_track={},
                    history=(),
                    transitions=(),
                    last_received_at=None,
                    total_events=0,
                    all_latest_by_track={},
                )

            active_tracks = {
                track_id: event
                for (sid, track_id), event in self._latest_by_track.items()
                if sid == session_id
                and (current_time - self._received_by_track[(sid, track_id)]).total_seconds()
                <= self._stale_track_seconds
            }
            all_tracks = {
                track_id: event
                for (sid, track_id), event in self._all_latest_by_track.items()
                if sid == session_id
            }
            return DashboardSnapshot(
                session_id=session_id,
                latest_by_track=active_tracks,
                history=tuple(self._history[session_id]),
                transitions=tuple(self._transitions[session_id]),
                last_received_at=self._last_received_at.get(session_id),
                total_events=self._total_events.get(session_id, 0),
                all_latest_by_track=all_tracks,
            )

    def snapshot_all(self, now: datetime | None = None) -> AllSessionsSnapshot:
        """Read-model for every session combined ("All" option in the
        Analytics tab). See AllSessionsSnapshot's docstring for why every
        entry is tagged with its session_id instead of being collapsed onto
        a bare track_id like snapshot() does."""
        current_time = now or datetime.now(timezone.utc)
        with self._lock:
            latest_events = tuple(
                event
                for (sid, track_id), event in self._latest_by_track.items()
                if (current_time - self._received_by_track[(sid, track_id)]).total_seconds()
                <= self._stale_track_seconds
            )
            all_latest_events = tuple(
                (sid, event) for (sid, _track_id), event in self._all_latest_by_track.items()
            )
            merged_history = tuple(
                sorted(
                    (
                        (sid, event)
                        for sid in self._session_order
                        for event in self._history[sid]
                    ),
                    key=lambda pair: pair[1].timestamp,
                )
            )
            received_ats = [t for t in self._last_received_at.values() if t is not None]
            last_received_at = max(received_ats) if received_ats else None
            total_events = sum(self._total_events.values())

            return AllSessionsSnapshot(
                session_ids=tuple(self._session_order),
                latest_events=latest_events,
                all_latest_events=all_latest_events,
                history=merged_history,
                last_received_at=last_received_at,
                total_events=total_events,
            )

    def list_sessions(self) -> list[str]:
        """Every session ID seen so far, oldest to newest."""
        with self._lock:
            return list(self._session_order)
