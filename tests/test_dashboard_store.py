from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from dashboard.models import DashboardEvent
from dashboard.store import EventStore


def make_event(frame_id: int, state: str = "IDLE") -> DashboardEvent:
    return DashboardEvent.model_validate(
        {
            "track_id": 1,
            "timestamp": datetime.now(timezone.utc),
            "frame_id": frame_id,
            "video_time_seconds": float(frame_id),
            "state": state,
            "motion_score": 0.5,
            "bbox": [0, 0, 10, 10],
            "working_seconds": float(frame_id if state == "WORKING" else 0),
            "idle_seconds": float(frame_id if state == "IDLE" else 0),
        }
    )


class EventStoreTests(unittest.TestCase):
    def test_history_is_bounded_and_latest_event_is_preserved(self) -> None:
        store = EventStore(max_events=2)

        store.append(make_event(1))
        store.append(make_event(2))
        store.append(make_event(3, state="WORKING"))

        snapshot = store.snapshot()
        self.assertEqual([event.frame_id for event in snapshot.history], [2, 3])
        self.assertEqual(snapshot.latest_by_track[1].frame_id, 3)
        self.assertEqual(snapshot.total_events, 3)

    def test_transition_history_ignores_repeated_states(self) -> None:
        store = EventStore()

        store.append(make_event(1, state="IDLE"))
        store.append(make_event(2, state="IDLE"))
        store.append(make_event(3, state="WORKING"))

        transitions = store.snapshot().transitions
        self.assertEqual([event.frame_id for event in transitions], [1, 3])

    def test_stale_tracks_leave_current_view_but_history_is_retained(self) -> None:
        store = EventStore(stale_track_seconds=5.0)
        received_at = datetime.now(timezone.utc)
        store.append(make_event(1), received_at=received_at)

        snapshot = store.snapshot(now=received_at + timedelta(seconds=6))

        self.assertEqual(snapshot.latest_by_track, {})
        self.assertEqual(len(snapshot.history), 1)


if __name__ == "__main__":
    unittest.main()
