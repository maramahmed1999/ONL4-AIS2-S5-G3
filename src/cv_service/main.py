"""
cv_service/main.py
──────────────────
Entry point for the excavator CV pipeline.

Pipeline per-frame:
  VideoCapture → frame_skip → OpticalFlow.update()
      → ExcavatorDetector.detect()
      → [per detection] OpticalFlow.is_working() → StateMachine.update()
      → EventProducer.send()

Run:
    python cv_service/main.py                     # uses default_video_path from settings
    python cv_service/main.py data/site_cam1.mp4  # override path
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import cv2
import numpy as np

# Allow running from project root: `python cv_service/main.py`
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import settings
from cv_service.detector import ExcavatorDetector
from cv_service.kafka_producer import EventProducer
from cv_service.motion import OpticalFlowAnalyzer
from cv_service.state_machine import EquipmentState, EquipmentStateMachine, StateRecord

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

STATE_COLOR_BGR: dict[str, tuple[int, int, int]] = {
    "WORKING": (50, 205, 50),
    "MOVING": (0, 165, 255),
    "IDLE": (60, 60, 200),
}

# ── Event schema ──────────────────────────────────────────────────────────────

def build_event(
    track_id: int,
    state: EquipmentState,
    record: StateRecord,
    motion_score: float,
    bbox: tuple[int, int, int, int],
    frame_id: int,
    video_time_seconds: float,
) -> dict:
    """
    Canonical Kafka event payload.
    All consumers depend on this schema — keep it stable and additive only.
    """
    return {
        "track_id": track_id,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "frame_id": frame_id,
        "video_time_seconds": round(video_time_seconds, 3),
        "state": state.value,
        "motion_score": motion_score,
        "bbox": list(bbox),                          # [x1, y1, x2, y2]
        "working_seconds": round(record.working_seconds, 2),
        "moving_seconds": round(record.moving_seconds, 2),
        "idle_seconds": round(record.idle_seconds, 2),
    }


def annotate_frame(
    frame: np.ndarray,
    detections: list,
    state_machine: EquipmentStateMachine,
    frame_id: int,
    video_time_seconds: float,
) -> np.ndarray:
    out = frame.copy()
    font = cv2.FONT_HERSHEY_SIMPLEX

    for det in detections:
        record = state_machine.get_record(det.track_id)
        if record is None:
            continue

        state_name = record.state.value
        color = STATE_COLOR_BGR.get(state_name, (200, 200, 200))
        x1, y1, x2, y2 = det.bbox
        cv2.rectangle(out, (x1, y1), (x2, y2), color, thickness=2)

        lines = [
            f"ID:{det.track_id} {state_name}",
            f"W:{record.working_seconds:.0f}s M:{record.moving_seconds:.0f}s I:{record.idle_seconds:.0f}s",
        ]
        text_width = max(cv2.getTextSize(line, font, 0.5, 1)[0][0] for line in lines)
        top = max(0, y1 - 44)
        cv2.rectangle(out, (x1, top), (x1 + text_width + 8, top + 42), color, -1)
        for index, line in enumerate(lines):
            cv2.putText(
                out,
                line,
                (x1 + 4, top + 16 + index * 18),
                font,
                0.5,
                (255, 255, 255),
                1,
                cv2.LINE_AA,
            )

    cv2.putText(
        out,
        f"Frame {frame_id} | Video {video_time_seconds:.1f}s",
        (10, out.shape[0] - 12),
        font,
        0.5,
        (230, 230, 230),
        1,
        cv2.LINE_AA,
    )
    return out


def write_preview_frame(frame: np.ndarray, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_name(
        f"{output_path.stem}.{time.monotonic_ns()}.tmp{output_path.suffix}"
    )
    ok, encoded = cv2.imencode(
        ".jpg",
        frame,
        [cv2.IMWRITE_JPEG_QUALITY, settings.preview_jpeg_quality],
    )
    if not ok:
        logger.warning("Could not encode preview frame")
        return
    tmp_path.write_bytes(encoded.tobytes())
    for _ in range(5):
        try:
            tmp_path.replace(output_path)
            return
        except PermissionError:
            time.sleep(0.03)

    logger.warning("Preview frame is locked; skipped this preview update")
    try:
        tmp_path.unlink(missing_ok=True)
    except OSError:
        pass


# ── Pipeline ──────────────────────────────────────────────────────────────────

def run_pipeline(video_path: str) -> None:
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video source: {video_path!r}")

    source_fps: float = cap.get(cv2.CAP_PROP_FPS) or 25.0
    total_frames: int = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    frame_skip: int = max(1, round(source_fps / settings.target_fps))

    logger.info(
        f"Video opened — source FPS: {source_fps:.1f}, "
        f"processing every {frame_skip} frame(s) → ~{source_fps / frame_skip:.1f} FPS, "
        f"total frames: {total_frames}"
    )

    # ── Component initialisation (singletons — created once) ──────────────────
    detector = ExcavatorDetector(
        model_path=settings.resolve_path(settings.model_path),
        conf_threshold=settings.conf_threshold,
        iou_threshold=settings.iou_threshold,
        imgsz=settings.detection_imgsz,
        device=settings.yolo_device,
    )
    flow_analyzer = OpticalFlowAnalyzer(
        arm_region_ratio=settings.arm_region_ratio,
        magnitude_threshold=settings.motion_magnitude_threshold,
        max_width=settings.optical_flow_max_width,
    )
    state_machine = EquipmentStateMachine(
        move_threshold_pixels=settings.move_threshold_pixels,
        frames_to_confirm=settings.frames_to_confirm,
        stale_timeout=settings.stale_track_timeout,
    )
    producer = EventProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic,
    )
    preview_path = settings.resolve_path(settings.preview_frame_path)

    frame_id: int = 0
    processed: int = 0
    recent_motion_scores: list[float] = []
    last_detections: list = []

    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                logger.info("End of video stream — pipeline complete.")
                break

            frame_id += 1
            video_time_seconds = cap.get(cv2.CAP_PROP_POS_MSEC) / 1000.0
            if video_time_seconds <= 0:
                video_time_seconds = frame_id / source_fps

            flow_ready = flow_analyzer.update(frame)

            if frame_id % frame_skip != 0:
                flow_analyzer.finish_frame()
                continue

            processed += 1

            if not flow_ready:
                flow_analyzer.finish_frame()
                continue

            # ── Detection + tracking ──────────────────────────────────────────
            detection_interval = max(1, settings.detection_every_n_processed_frames)
            if (processed - 1) % detection_interval == 0:
                detections = detector.detect(frame)
                last_detections = detections
            else:
                detections = last_detections

            if not detections:
                logger.debug(f"Frame {frame_id}: no detections.")

            # ── Per-detection: flow analysis → state update → publish ──────────
            for det in detections:
                is_working, motion_score = flow_analyzer.is_working(flow_ready, det.bbox)
                recent_motion_scores.append(motion_score)

                state, state_changed = state_machine.update(
                    track_id=det.track_id,
                    bbox=det.bbox,
                    is_working_signal=is_working,
                    motion_score=motion_score,
                    video_time_seconds=video_time_seconds,
                )

                record = state_machine.get_record(det.track_id)
                if record is None:
                    continue

                event = build_event(
                    det.track_id, state, record, motion_score,
                    det.bbox, frame_id, video_time_seconds,
                )
                producer.send(event)

                if state_changed:
                    logger.info(
                        f"[Track {det.track_id}] {state.value} "
                        f"| motion={motion_score:.3f} "
                        f"| working={record.working_seconds:.1f}s "
                        f"| moving={record.moving_seconds:.1f}s "
                        f"| idle={record.idle_seconds:.1f}s"
                    )

            preview_interval = max(1, settings.preview_every_n_processed_frames)
            if settings.preview_enabled and processed % preview_interval == 0:
                annotated = annotate_frame(frame, detections, state_machine, frame_id, video_time_seconds)
                write_preview_frame(annotated, preview_path)

            flow_analyzer.finish_frame()

            if processed % 100 == 0:
                stale = state_machine.purge_stale_tracks()
                motion_summary = "no detections"
                if recent_motion_scores:
                    motion_summary = (
                        f"motion avg={sum(recent_motion_scores) / len(recent_motion_scores):.3f}, "
                        f"max={max(recent_motion_scores):.3f}, "
                        f"threshold={settings.motion_magnitude_threshold:.3f}"
                    )
                    recent_motion_scores.clear()
                logger.info(
                    f"Progress: {frame_id}/{total_frames} frames read, "
                    f"{processed} processed. "
                    f"Active tracks: {len(state_machine.get_all_records())}. "
                    f"Purged: {stale or 'none'}. "
                    f"{motion_summary}."
                )

    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    finally:
        cap.release()
        logger.info("Flushing Kafka producer...")
        producer.flush()
        logger.info("Shutdown complete.")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    video_path = sys.argv[1] if len(sys.argv) > 1 else str(settings.resolve_path(settings.default_video_path))
    logger.info(f"Starting pipeline with video: {video_path!r}")
    run_pipeline(video_path)
