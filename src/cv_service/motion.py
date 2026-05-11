from __future__ import annotations

import logging

import cv2
import numpy as np

logger = logging.getLogger(__name__)

_FARNEBACK_PARAMS = dict(
    pyr_scale=0.5,
    levels=2,
    winsize=11,
    iterations=1,
    poly_n=5,
    poly_sigma=1.1,
    flags=0,
)


class OpticalFlowAnalyzer:
    def __init__(
        self,
        arm_region_ratio: float = 0.65,
        magnitude_threshold: float = 1.5,
        max_width: int | None = 320,
    ) -> None:
        if not 0.0 < arm_region_ratio <= 1.0:
            raise ValueError("arm_region_ratio must be in (0, 1]")

        self._arm_ratio = arm_region_ratio
        self._threshold = magnitude_threshold
        self._max_width = max_width if max_width and max_width > 0 else None
        self._scale_x = 1.0
        self._scale_y = 1.0
        self._prev_gray: np.ndarray | None = None
        self._curr_gray: np.ndarray | None = None

    def set_magnitude_threshold(self, magnitude_threshold: float) -> None:
        self._threshold = magnitude_threshold

    def update(self, frame: np.ndarray) -> bool:
        gray = self._to_flow_gray(frame)
        if self._prev_gray is None:
            self._prev_gray = gray
            self._curr_gray = None
            return False
        self._curr_gray = gray
        return True

    def finish_frame(self) -> None:
        if self._curr_gray is not None:
            self._prev_gray = self._curr_gray
            self._curr_gray = None

    def get_arm_region_score(self, bbox: tuple[int, int, int, int]) -> float:
        if self._prev_gray is None or self._curr_gray is None:
            return 0.0

        x1, y1, x2, y2 = self._scaled_arm_region(bbox, self._curr_gray.shape)
        if x2 <= x1 or y2 <= y1:
            return 0.0

        prev_roi = self._prev_gray[y1:y2, x1:x2]
        curr_roi = self._curr_gray[y1:y2, x1:x2]
        if prev_roi.shape[0] < 4 or prev_roi.shape[1] < 4:
            return 0.0

        flow = cv2.calcOpticalFlowFarneback(prev_roi, curr_roi, None, **_FARNEBACK_PARAMS)
        magnitude, _ = cv2.cartToPolar(flow[..., 0], flow[..., 1])
        
        # Use 90th percentile instead of mean — much less sensitive to noise
        score = float(np.percentile(magnitude, 90))
        return score

    def is_working(self, _flow_ready: object, bbox: tuple[int, int, int, int]) -> tuple[bool, float]:
        score = self.get_arm_region_score(bbox)
        return score > self._threshold, round(score, 4)

    def _to_flow_gray(self, frame: np.ndarray) -> np.ndarray:
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        height, width = gray.shape[:2]

        if self._max_width is None or width <= self._max_width:
            self._scale_x = 1.0
            self._scale_y = 1.0
            return gray

        scale = self._max_width / width
        resized_width = self._max_width
        resized_height = max(1, round(height * scale))
        self._scale_x = resized_width / width
        self._scale_y = resized_height / height
        return cv2.resize(gray, (resized_width, resized_height), interpolation=cv2.INTER_AREA)

    def _scaled_arm_region(
        self,
        bbox: tuple[int, int, int, int],
        frame_shape: tuple[int, int],
    ) -> tuple[int, int, int, int]:
        x1, y1, x2, y2 = bbox
        frame_h, frame_w = frame_shape[:2]

        x1 = int(x1 * self._scale_x)
        x2 = int(x2 * self._scale_x)
        y1 = int(y1 * self._scale_y)
        y2 = int(y2 * self._scale_y)

        arm_y2 = y1 + int((y2 - y1) * self._arm_ratio)

        x1 = max(0, min(x1, frame_w))
        x2 = max(0, min(x2, frame_w))
        y1 = max(0, min(y1, frame_h))
        arm_y2 = max(0, min(arm_y2, frame_h))
        return x1, y1, x2, arm_y2