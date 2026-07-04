from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from dashboard.models import DashboardEvent
from dashboard.services.metrics import calculate_metrics
from dashboard.store import DashboardSnapshot


class DashboardMetricsTests(unittest.TestCase):
    def test_calculates_aggregate_utilization(self) -> None:
        event = DashboardEvent.model_validate(
            {
                "track_id": 1,
                "timestamp": datetime.now(timezone.utc),
                "frame_id": 10,
                "video_time_seconds": 10.0,
                "state": "WORKING",
                "motion_score": 1.0,
                "bbox": [0, 0, 10, 10],
                "working_seconds": 8.0,
                "idle_seconds": 2.0,
            }
        )
        now = datetime.now(timezone.utc)
        snapshot = DashboardSnapshot(
            latest_by_track={1: event},
            history=(event,),
            transitions=(event,),
            last_received_at=now,
            total_events=1,
        )

        metrics = calculate_metrics(snapshot, now=now)

        self.assertEqual(metrics.tracked_equipment, 1)
        self.assertEqual(metrics.working_equipment, 1)
        self.assertEqual(metrics.idle_equipment, 0)
        self.assertAlmostEqual(metrics.utilization_percent, 80.0)
        self.assertAlmostEqual(metrics.event_lag_seconds, 0.0)


if __name__ == "__main__":
    unittest.main()
