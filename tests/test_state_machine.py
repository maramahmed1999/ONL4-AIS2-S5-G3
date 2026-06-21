from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from cv_service.state_machine import EquipmentState, EquipmentStateMachine


class EquipmentStateMachineTests(unittest.TestCase):
    def test_uses_video_time_for_duration_accounting(self) -> None:
        machine = EquipmentStateMachine(frames_to_confirm=1)

        machine.update(
            track_id=1,
            bbox=(0, 0, 10, 10),
            is_working_signal=False,
            motion_score=0.0,
            video_time_seconds=0.0,
        )
        state, changed = machine.update(
            track_id=1,
            bbox=(100, 0, 110, 10),
            is_working_signal=False,
            motion_score=0.0,
            video_time_seconds=2.5,
        )

        record = machine.get_record(1)
        self.assertIsNotNone(record)
        self.assertEqual(state, EquipmentState.WORKING)
        self.assertTrue(changed)
        self.assertAlmostEqual(record.idle_seconds, 2.5)
        self.assertAlmostEqual(record.working_seconds, 0.0)

        machine.update(
            track_id=1,
            bbox=(200, 0, 210, 10),
            is_working_signal=False,
            motion_score=0.0,
            video_time_seconds=4.0,
        )
        self.assertAlmostEqual(record.working_seconds, 1.5)

    def test_debounce_requires_consecutive_signals(self) -> None:
        machine = EquipmentStateMachine(frames_to_confirm=3)

        for index in range(2):
            state, changed = machine.update(
                track_id=1,
                bbox=(0, 0, 10, 10),
                is_working_signal=True,
                motion_score=1.0,
                video_time_seconds=float(index),
            )

        self.assertEqual(state, EquipmentState.IDLE)
        self.assertFalse(changed)

        state, changed = machine.update(
            track_id=1,
            bbox=(0, 0, 10, 10),
            is_working_signal=True,
            motion_score=1.0,
            video_time_seconds=2.0,
        )

        self.assertEqual(state, EquipmentState.WORKING)
        self.assertTrue(changed)

    def test_working_and_displacement_share_one_confirmation_streak(self) -> None:
        machine = EquipmentStateMachine(
            move_threshold_pixels=15.0,
            frames_to_confirm=2,
        )

        state, changed = machine.update(
            track_id=1,
            bbox=(0, 0, 10, 10),
            is_working_signal=True,
            motion_score=1.0,
            video_time_seconds=0.0,
        )
        self.assertEqual(state, EquipmentState.IDLE)
        self.assertFalse(changed)

        state, changed = machine.update(
            track_id=1,
            bbox=(100, 0, 110, 10),
            is_working_signal=False,
            motion_score=0.0,
            video_time_seconds=1.0,
        )
        self.assertEqual(state, EquipmentState.WORKING)
        self.assertTrue(changed)

    def test_stale_tracks_are_purged(self) -> None:
        machine = EquipmentStateMachine(frames_to_confirm=1, stale_timeout=0.001)
        machine.update(
            track_id=1,
            bbox=(0, 0, 10, 10),
            is_working_signal=False,
            motion_score=0.0,
        )

        record = machine.get_record(1)
        self.assertIsNotNone(record)
        record.last_seen_at -= 1.0

        self.assertEqual(machine.purge_stale_tracks(), [1])
        self.assertIsNone(machine.get_record(1))


if __name__ == "__main__":
    unittest.main()
