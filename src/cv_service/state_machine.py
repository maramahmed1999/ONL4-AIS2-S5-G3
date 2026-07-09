from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class EquipmentState(str, Enum):
    IDLE = "IDLE"
    WORKING = "WORKING"


@dataclass
class StateRecord:

   
    state: EquipmentState = EquipmentState.IDLE

    # Time accounting (seconds spent in each state)
    working_seconds: float = 0.0
    idle_seconds: float = 0.0

    # Internal timestamps
    last_seen_at: float = field(default_factory=time.monotonic)
    last_video_time_seconds: float | None = None

    # Debounce frame counters — prevent noisy single-frame state flips
    _working_streak: int = field(default=0, repr=False)
    _idle_streak: int = field(default=0, repr=False)


class EquipmentStateMachine:

    def __init__(
        self,
        move_threshold_pixels: float = 20.0,
        frames_to_confirm: int = 5,
        stale_timeout: float = 10.0,
    ) -> None:
        self._move_threshold = move_threshold_pixels
        self._frames_to_confirm = frames_to_confirm
        self._stale_timeout = stale_timeout

        self._records: dict[int, StateRecord] = {}
        self._prev_centroids: dict[int, tuple[float, float]] = {}


    def set_move_threshold(self, move_threshold_pixels: float) -> None:
        self._move_threshold = move_threshold_pixels

    def update(self, track_id, bbox, is_working_signal, motion_score, video_time_seconds=None):
        record = self._get_or_create(track_id)
        now = time.monotonic()

        elapsed = self._elapsed_seconds(record, now, video_time_seconds)
        record.last_seen_at = now

        cx, cy = self._centroid(bbox)
        displacement = self._update_centroid(track_id, cx, cy)
        is_displacement_signal = displacement > self._move_threshold
        is_active_signal = is_working_signal or is_displacement_signal

        if is_active_signal:
            record._working_streak += 1
            record._idle_streak = 0
        else:
            record._idle_streak += 1
            record._working_streak = 0

        n = self._frames_to_confirm  # frame-confirmation threshold
        prev_state = record.state

        self._accumulate_time(record, elapsed)

        if record._working_streak >= n:
            record.state = EquipmentState.WORKING
        elif record._idle_streak >= n:
            record.state = EquipmentState.IDLE

        state_changed = record.state != prev_state
        print(
        f"track={track_id}, "
        f"working_signal={is_working_signal}, "
        f"motion_score={motion_score}"
        )
        return record.state, state_changed

    def get_record(self, track_id: int) -> StateRecord | None:
        return self._records.get(track_id)

    def get_all_records(self) -> dict[int, StateRecord]:
        return dict(self._records)

    def purge_stale_tracks(self) -> list[int]:
        
        now = time.monotonic()
        stale_ids = [
            tid
            for tid, rec in self._records.items()
            if now - rec.last_seen_at > self._stale_timeout
        ]
        for tid in stale_ids:
            del self._records[tid]
            self._prev_centroids.pop(tid, None)
            logger.info(f"Purged stale track: {tid}")

        return stale_ids

    def _get_or_create(self, track_id: int) -> StateRecord:
        if track_id not in self._records:
            self._records[track_id] = StateRecord()
            logger.info(f"New track registered: {track_id}")
        return self._records[track_id]

    @staticmethod
    def _centroid(bbox: tuple[int, int, int, int]) -> tuple[float, float]:
        x1, y1, x2, y2 = bbox
        return (x1 + x2) / 2.0, (y1 + y2) / 2.0

    def _update_centroid(self, track_id: int, cx: float, cy: float) -> float:
        """Returns Euclidean displacement from previous centroid, then updates it."""
        if track_id not in self._prev_centroids:
            self._prev_centroids[track_id] = (cx, cy)
            return 0.0
        px, py = self._prev_centroids[track_id]
        displacement = ((cx - px) ** 2 + (cy - py) ** 2) ** 0.5
        self._prev_centroids[track_id] = (cx, cy)
        return displacement

    @staticmethod
    def _elapsed_seconds(
        record: StateRecord,
        now: float,
        video_time_seconds: float | None,
    ) -> float:
        if video_time_seconds is None:
            return max(0.0, now - record.last_seen_at)

        if record.last_video_time_seconds is None:
            record.last_video_time_seconds = video_time_seconds
            return 0.0

        elapsed = video_time_seconds - record.last_video_time_seconds
        record.last_video_time_seconds = video_time_seconds
        return max(0.0, elapsed)

    @staticmethod
    def _accumulate_time(record: StateRecord, elapsed: float) -> None:
        if record.state == EquipmentState.WORKING:
            record.working_seconds += elapsed
        else:
            record.idle_seconds += elapsed
