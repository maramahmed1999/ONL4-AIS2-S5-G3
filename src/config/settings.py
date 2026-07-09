from pathlib import Path
from pydantic_settings import BaseSettings

SRC_ROOT = Path(__file__).resolve().parents[1]

class Settings(BaseSettings):
    # Video
    default_video_path: str = "dataset/video2.mp4"
    target_fps: float = 30.0
    preview_enabled: bool = True
    preview_frame_path: str = "runtime/latest_frame.jpg"
    preview_jpeg_quality: int = 80
    preview_every_n_processed_frames: int = 3

    # Detection
    model_path: str = "cv_service/models/best.pt"
    detection_imgsz: int = 640
    detection_every_n_processed_frames: int = 1
    yolo_device: str | None = "0"
    conf_threshold: float = 0.4
    iou_threshold: float = 0.5

    # Optical flow — raised threshold + percentile scoring
    arm_region_ratio: float = 1.0
    optical_flow_max_width: int = 320
    motion_magnitude_threshold: float = 0.55

    # State machine
    move_threshold_pixels: float = 15
    frames_to_confirm: int = 5
    stale_track_timeout: float = 10.0

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "excavator_events"
    kafka_consumer_group_id: str = "dashboard-consumer-group"

    # Model performance monitoring (System Monitoring tab)
    model_metrics_interval_seconds: float = 5.0        # how often cv_service rolls up + persists a metrics sample
    hard_frames_dir: str = "runtime/hard_frames"        # where low-confidence frames are saved for later review
    hard_frame_conf_threshold: float = 0.5              # per-detection confidence below this is saved as a "hard frame"
    hard_frame_min_interval_seconds: float = 2.0        # throttle: don't save more than one hard frame this often
    low_confidence_alert_percent: float = 60.0          # System Monitoring warning threshold (avg confidence)
    no_detections_alert_minutes: float = 10.0           # System Monitoring warning threshold (detection silence)

    model_config = {
        "env_file": str(SRC_ROOT / ".env"),
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }

    def resolve_path(self, path_value: str | Path) -> Path:
        path = Path(path_value)
        return path if path.is_absolute() else SRC_ROOT / path

settings = Settings()