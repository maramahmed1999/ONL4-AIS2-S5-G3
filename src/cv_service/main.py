"""
cv_service/main.py
──────────────────
Real-time excavator CV pipeline with native playback speed.
- Capture Thread: reads video at exactly source FPS (no faster).
- OpenCV Window: smooth native-speed preview.
"""

from __future__ import annotations

import logging
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from queue import Queue, Empty, Full

import cv2
import numpy as np

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
    "IDLE": (60, 60, 200),
}


# ── Capture Thread (reads at native FPS, never faster) ───────────────────────

class CaptureThread(threading.Thread):
    def __init__(self, source: str | int, queue_size: int = 60):
        super().__init__(daemon=True, name="capture-thread")
        self.cap = cv2.VideoCapture(source)
        if not self.cap.isOpened():
            raise RuntimeError(f"Cannot open video: {source!r}")
        
        self.source_fps = self.cap.get(cv2.CAP_PROP_FPS) or 25.0
        self.total_frames = int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT))
        self.queue: Queue[tuple[int, np.ndarray, float]] = Queue(maxsize=queue_size)
        self._shutdown = threading.Event()
        self._frame_id = 0

    def run(self):
        frame_interval = 1.0 / self.source_fps
        next_frame_time = time.perf_counter()
        
        while not self._shutdown.is_set():
            ret, frame = self.cap.read()
            if not ret:
                logger.info("Capture thread: end of video.")
                break
            
            self._frame_id += 1
            video_time = self.cap.get(cv2.CAP_PROP_POS_MSEC) / 1000.0
            if video_time <= 0:
                video_time = self._frame_id / self.source_fps
            
            if self.queue.full():
                try:
                    self.queue.get_nowait()
                except Empty:
                    pass
            
            try:
                self.queue.put_nowait((self._frame_id, frame, video_time))
            except Full:
                pass
            
            # Sleep to maintain native FPS (e.g., 33ms for 30 FPS)
            next_frame_time += frame_interval
            sleep_time = next_frame_time - time.perf_counter()
            if sleep_time > 0:
                time.sleep(sleep_time)

    def get(self) -> tuple[int, np.ndarray, float] | None:
        try:
            return self.queue.get(timeout=0.05)
        except Empty:
            return None

    def release(self):
        self._shutdown.set()
        self.join(timeout=2.0)
        self.cap.release()

    def is_open(self) -> bool:
        return not self._shutdown.is_set() or not self.queue.empty()


# ── Background Workers ───────────────────────────────────────────────────────

class KafkaWorker(threading.Thread):
    def __init__(self, producer: EventProducer, max_queue: int = 2000):
        super().__init__(daemon=True, name="kafka-worker")
        self.producer = producer
        self.queue: Queue[dict] = Queue(maxsize=max_queue)
        self._shutdown = threading.Event()

    def send(self, event: dict) -> None:
        try:
            self.queue.put_nowait(event)
        except Full:
            pass

    def run(self):
        while not self._shutdown.is_set():
            try:
                event = self.queue.get(timeout=0.05)
            except Empty:
                self.producer._producer.poll(0)
                continue
            self.producer.send(event)

    def stop(self):
        self._shutdown.set()
        self.join(timeout=2.0)
        self.producer.flush(timeout=5.0)


class PreviewWorker(threading.Thread):
    def __init__(self, output_path: Path, quality: int, max_queue: int = 2):
        super().__init__(daemon=True, name="preview-worker")
        self.output_path = output_path
        self.quality = quality
        self.queue: Queue[np.ndarray] = Queue(maxsize=max_queue)
        self._shutdown = threading.Event()

    def submit(self, frame: np.ndarray) -> None:
        try:
            self.queue.put_nowait(frame)
        except Full:
            pass

    def run(self):
        while not self._shutdown.is_set():
            try:
                frame = self.queue.get(timeout=0.05)
            except Empty:
                continue
            self._write(frame)

    def _write(self, frame: np.ndarray) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.output_path.with_name(f"{self.output_path.stem}.{time.monotonic_ns()}.tmp.jpg")
        ok, encoded = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, self.quality])
        if ok:
            tmp.write_bytes(encoded.tobytes())
            for _ in range(5):
                try:
                    tmp.replace(self.output_path)
                    return
                except PermissionError:
                    time.sleep(0.03)
            tmp.unlink(missing_ok=True)

    def stop(self):
        self._shutdown.set()
        self.join(timeout=1.0)


# ── Helpers ──────────────────────────────────────────────────────────────────

def build_event(track_id, state, record, motion_score, bbox, frame_id, video_time):
    return {
        "track_id": track_id,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "frame_id": frame_id,
        "video_time_seconds": round(video_time, 3),
        "state": state.value,
        "motion_score": motion_score,
        "bbox": list(bbox),
        "working_seconds": round(record.working_seconds, 2),
        "idle_seconds": round(record.idle_seconds, 2),
    }


def annotate_frame(frame, detections, state_machine, frame_id, video_time):
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
            f"W:{record.working_seconds:.0f}s I:{record.idle_seconds:.0f}s",
        ]
        text_width = max(cv2.getTextSize(line, font, 0.5, 1)[0][0] for line in lines)
        top = max(0, y1 - 44)
        cv2.rectangle(out, (x1, top), (x1 + text_width + 8, top + 42), color, -1)
        for index, line in enumerate(lines):
            cv2.putText(out, line, (x1 + 4, top + 16 + index * 18),
                        font, 0.5, (255, 255, 255), 1, cv2.LINE_AA)

    cv2.putText(out, f"Frame {frame_id} | {video_time:.2f}s",
                (10, out.shape[0] - 12), font, 0.5, (230, 230, 230), 1, cv2.LINE_AA)
    return out


# ── Pipeline ──────────────────────────────────────────────────────────────────

def run_pipeline(video_path: str) -> None:
    capture = CaptureThread(video_path, queue_size=60)
    capture.start()

    source_fps = capture.source_fps
    logger.info(
        f"Pipeline start — source: {source_fps:.1f} FPS | "
        f"GPU: {settings.yolo_device or 'cpu'} | "
        f"Playback: NATIVE SPEED (frame-rate controlled)"
    )

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
    kafka_worker = KafkaWorker(producer, max_queue=2000)
    kafka_worker.start()

    preview_path = settings.resolve_path(settings.preview_frame_path)
    preview_worker = PreviewWorker(preview_path, settings.preview_jpeg_quality)
    preview_worker.start()

    processed = 0
    last_detections: list = []
    fps_start = time.perf_counter()
    fps_frames = 0

    window_name = "Excavator Monitor"
    cv2.namedWindow(window_name, cv2.WINDOW_NORMAL)

    try:
        while True:
            item = capture.get()
            if item is None:
                if not capture.is_open():
                    logger.info("Processing complete.")
                    break
                continue

            frame_id, frame, video_time = item

            flow_ready = flow_analyzer.update(frame)
            if not flow_ready:
                flow_analyzer.finish_frame()
                continue

            processed += 1

            det_interval = max(1, settings.detection_every_n_processed_frames)
            if (processed - 1) % det_interval == 0:
                last_detections = detector.detect(frame)
            detections = last_detections

            for det in detections:
                is_working, motion_score = flow_analyzer.is_working(flow_ready, det.bbox)

                state, changed = state_machine.update(
                    track_id=det.track_id,
                    bbox=det.bbox,
                    is_working_signal=is_working,
                    motion_score=motion_score,
                    video_time_seconds=video_time,
                )

                record = state_machine.get_record(det.track_id)
                if record is None:
                    continue

                if changed or processed % 30 == 0:
                    event = build_event(
                        det.track_id, state, record, motion_score,
                        det.bbox, frame_id, video_time,
                    )
                    kafka_worker.send(event)

                # Log motion scores for tuning (first 5 detections or on change)
                if changed or processed <= 5:
                    logger.info(
                        f"[T{det.track_id}] {state.value} | "
                        f"motion_score={motion_score:.3f} | "
                        f"threshold={settings.motion_magnitude_threshold:.3f}"
                    )

            annotated = annotate_frame(frame, detections, state_machine, frame_id, video_time)
            cv2.imshow(window_name, annotated)

            if cv2.waitKey(1) & 0xFF == ord('q'):
                logger.info("User pressed 'q' — stopping.")
                break

            preview_int = max(1, settings.preview_every_n_processed_frames)
            if settings.preview_enabled and processed % preview_int == 0:
                preview_worker.submit(annotated)

            flow_analyzer.finish_frame()

            fps_frames += 1
            if processed % 100 == 0:
                elapsed = time.perf_counter() - fps_start
                actual_fps = fps_frames / elapsed if elapsed > 0 else 0
                queue_size = capture.queue.qsize()
                stale = state_machine.purge_stale_tracks()
                logger.info(
                    f"Processing FPS={actual_fps:.1f} | "
                    f"processed={processed} | "
                    f"capture_queue={queue_size} | "
                    f"tracks={len(state_machine.get_all_records())}"
                )
                fps_start = time.perf_counter()
                fps_frames = 0

    except KeyboardInterrupt:
        logger.info("Interrupted.")
    finally:
        cv2.destroyAllWindows()
        capture.release()
        kafka_worker.stop()
        preview_worker.stop()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    video_path = sys.argv[1] if len(sys.argv) > 1 else str(settings.resolve_path(settings.default_video_path))
    logger.info(f"Starting: {video_path!r}")
    run_pipeline(video_path)
