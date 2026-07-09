from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from ultralytics import YOLO

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Detection:
    track_id: int
    bbox: tuple[int, int, int, int]   # x1, y1, x2, y2 in pixel coords
    confidence: float
    class_name: str


class ExcavatorDetector:
    """
    Wraps YOLO26n with ByteTrack to produce persistent track IDs per excavator.

    Design notes:
    - Model is loaded ONCE at construction — never per-frame.
    - `persist=True` keeps ByteTrack state across calls so track IDs are stable.
    - Boxes with no assigned track ID (first frame edge-case) are silently dropped.
    """

    def __init__(
        self,
        model_path: str | Path,
        conf_threshold: float,
        iou_threshold: float,
        imgsz: int,
        device: str | None = None,
    ):
        
        model_path = Path(model_path)
        if not model_path.exists():
            raise FileNotFoundError(f"YOLO model not found: {model_path}")

        logger.info(f"Loading YOLO model from {model_path}")
        self._model = YOLO(str(model_path))
        self._conf = conf_threshold
        self._iou = iou_threshold
        self._imgsz = imgsz
        self._device = device or None

    def detect(self, frame: np.ndarray) -> list[Detection]:
        """
        Run detection + tracking on a single BGR frame.
        Returns a list of Detection objects (empty list if nothing detected).
        """
        track_kwargs = {
            "source": frame,
            "conf": self._conf,
            "iou": self._iou,
            "imgsz": self._imgsz,
            "persist": True,           # keeps ByteTrack state between calls
            "tracker": "bytetrack.yaml",
            "verbose": False,
            "stream": False,
            "half": False, 
        }

        results = self._model.track(**track_kwargs)

        detections: list[Detection] = []
        for r in results:
            if r.boxes is None or len(r.boxes) == 0:
                continue
            for box in r.boxes:
                if box.id is None:
                    # ByteTrack hasn't assigned an ID yet
                    continue
                x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())

                detections.append(
                    Detection(
                        track_id=int(box.id.item()),
                        bbox=(x1, y1, x2, y2),
                        confidence=float(box.conf.item()),
                        class_name=self._model.names[int(box.cls.item())],
                    )
                )

        return detections
