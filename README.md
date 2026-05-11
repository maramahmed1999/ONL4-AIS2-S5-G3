# Excavator Activity Monitor

Kafka-based excavator activity monitoring from video using YOLOv8 + ByteTrack,
Farneback optical flow, and a debounced state machine.

This project has one supported runtime architecture:

```text
cv_service/main.py
  -> reads video frames
  -> detects/tracks excavators with YOLOv8 + ByteTrack
  -> classifies each track as IDLE / WORKING / MOVING
  -> publishes JSON events to Kafka
  -> writes latest annotated preview frame to runtime/latest_frame.jpg

dashboard/app.py
  -> consumes Kafka events
  -> displays the latest preview frame
  -> displays live metrics, tables, charts, and state changes
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Expected local assets:

```text
cv_service/models/best.pt
dataset/excavator_03.mp4
```

Optional config:

```bash
cp .env.example .env
```

## Run

Start Kafka:

```bash
docker compose up -d
```

Terminal 1: start the CV producer:

```bash
python cv_service/main.py
```

Or pass a custom video:

```bash
python cv_service/main.py dataset/custom.mp4
```

Terminal 2: start the dashboard:

```bash
streamlit run dashboard/app.py
```

Open:

```text
http://localhost:8501
```

Kafka UI:

```text
http://localhost:9000
```

## State Logic

- `WORKING`: arm/bucket optical-flow motion exceeds the configured threshold.
- `MOVING`: bounding-box centroid displacement exceeds the movement threshold.
- `IDLE`: neither working nor moving is confirmed.

Transitions require `FRAMES_TO_CONFIRM` consecutive frames to reduce flicker.
Duration accounting uses source-video time, so metrics do not depend on how
fast the machine processes the video.

## Tuning

| Setting | Effect |
|---|---|
| `TARGET_FPS` | Number of frames per second to run detection and optical flow on. Lower is faster. |
| `PREVIEW_ENABLED` | Write latest annotated frame for the dashboard. |
| `PREVIEW_FRAME_PATH` | Preview image path read by the dashboard. |
| `PREVIEW_JPEG_QUALITY` | JPEG quality for the preview image. |
| `PREVIEW_EVERY_N_PROCESSED_FRAMES` | Write preview every N processed frames. Higher is faster. |
| `DETECTION_IMGSZ` | YOLO inference image size. Lower is faster. |
| `DETECTION_EVERY_N_PROCESSED_FRAMES` | Run YOLO every N processed frames and reuse the last tracks between runs. |
| `YOLO_DEVICE` | Set to `0` to use the first CUDA GPU, or leave unset for default behavior. |
| `CONF_THRESHOLD` | YOLO confidence cutoff. |
| `IOU_THRESHOLD` | YOLO/ByteTrack IoU threshold. |
| `ARM_REGION_RATIO` | Top fraction of bbox used as arm/bucket region. |
| `OPTICAL_FLOW_MAX_WIDTH` | Internal resize width for optical flow. Lower is faster. |
| `MOTION_MAGNITUDE_THRESHOLD` | Motion threshold for `WORKING`. |
| `MOVE_THRESHOLD_PIXELS` | Centroid shift threshold for `MOVING`. |
| `FRAMES_TO_CONFIRM` | Debounce window for state changes. |

If the dashboard stays mostly `IDLE`, lower `MOTION_MAGNITUDE_THRESHOLD`.
Watch the CV service logs for `motion avg`, `max`, and `threshold`; a good
threshold is usually between idle motion and active arm/bucket motion.

## Kafka Event Schema

```json
{
  "track_id": 1,
  "timestamp": "2026-05-08T10:23:01.123456+00:00",
  "frame_id": 450,
  "video_time_seconds": 18.0,
  "state": "WORKING",
  "motion_score": 2.314,
  "bbox": [120, 80, 340, 310],
  "working_seconds": 23.5,
  "moving_seconds": 4.1,
  "idle_seconds": 8.9
}
```

## Tests

```bash
python -m unittest discover -s tests
```
