# Excavator Activity Monitor

Kafka-based excavator activity monitoring from video using YOLOv8 + ByteTrack,
Farneback optical flow, and a debounced state machine.

For a complete architecture and file-by-file code explanation, see
[`PROJECT_TECHNICAL_GUIDE.md`](PROJECT_TECHNICAL_GUIDE.md).

This project has one supported runtime architecture:

```text
cv_service/main.py
  -> reads video frames (from an uploaded file or a live camera)
  -> detects/tracks excavators with YOLOv8 + ByteTrack
  -> classifies each track as IDLE / WORKING
  -> publishes JSON events to Kafka
  -> writes latest annotated preview frame to runtime/latest_frame.jpg

dashboard/app.py
  -> lets you choose Upload Video or Live Camera, and Start/Stop detection
     (launches cv_service/main.py as a managed background process)
  -> displays the live processed video (from runtime/latest_frame.jpg)
  -> consumes Kafka events
  -> displays live metrics, charts, an per-excavator summary table,
     and an Excel export of that summary
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r src/requirements.txt
```
## Run

Start Kafka:

```bash
docker compose -f src/docker-compose.yml up -d
```

Start the dashboard:

```bash
streamlit run src/dashboard/app.py
```

Open:

```text
http://localhost:8501
```

In the sidebar, choose **Upload Video** or **Live Camera**, then **Start
Detection**. This launches `cv_service/main.py` in the background with the
chosen source — you no longer need a separate terminal for it. **Stop
Detection** sends it a graceful shutdown signal (same as Ctrl+C), which
flushes any pending Kafka events before the process exits.

You can still run the CV pipeline manually in its own terminal instead, exactly as before:

```bash
python src/cv_service/main.py
python src/cv_service/main.py src/dataset/custom.mp4
```

Kafka UI:

```text
http://localhost:9000
```

## Dashboard Architecture

The Streamlit UI does not consume Kafka inside rendering functions. A single
cached `KafkaDashboardConsumer` runs in a background thread and writes validated
events into a thread-safe, bounded `EventStore`. The UI reads immutable snapshots
from the store on a timer, via `st.fragment(run_every=...)`.

```text
Kafka -> KafkaDashboardConsumer -> EventStore -> Streamlit components
cv_service/main.py (subprocess) <- PipelineManager <- sidebar controls
                                 -> runtime/latest_frame.jpg -> live preview
```

Dashboard responsibilities are separated under `src/dashboard/`:

- `models.py`: validates the Kafka event contract; defines `TrackSummary`
  and pipeline status/source models.
- `consumer.py`: owns Kafka polling and connection status.
- `store.py`: keeps latest equipment state (both currently-active and
  cumulative-per-track), bounded history, and transitions.
- `services/metrics.py`: calculates aggregate dashboard metrics and
  per-excavator summaries.
- `services/export.py`: builds the Excel summary workbook.
- `services/pipeline_manager.py`: starts/stops `cv_service/main.py` as a
  background process for an uploaded video or a live camera, and tails its log.
- `components/`: renders status, controls, live video, metrics, charts,
  and tables.
- `app.py`: wires dependencies and defines the page layout (Live Monitor /
  Analytics tabs plus sidebar controls).

## State Logic

- `WORKING`: arm/bucket optical-flow motion or bounding-box centroid displacement
  exceeds its configured threshold.
- `IDLE`: neither activity signal is confirmed.

Transitions require `FRAMES_TO_CONFIRM` consecutive frames to reduce flicker.
Duration accounting uses source-video time, so metrics do not depend on how
fast the machine processes the video.

## Tuning

| Setting | Effect |
|---|---|
| `TARGET_FPS` | Reserved configuration value; the current capture thread uses source-video FPS. |
| `PREVIEW_ENABLED` | Write the latest annotated frame for optional external consumers. |
| `PREVIEW_FRAME_PATH` | Output path for the optional preview image. |
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
| `MOVE_THRESHOLD_PIXELS` | Centroid shift threshold for detecting `WORKING` activity. |
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
  "idle_seconds": 8.9
}
```

## Tests

```bash
python -m unittest discover -s tests
```

## Notes on this update

Rebuilding the dashboard required two small changes outside `dashboard/`:

- **`cv_service/main.py`**: removed the `cv2.namedWindow` / `cv2.imshow` /
  `cv2.waitKey` / `cv2.destroyAllWindows` calls. `requirements.txt` pins
  `opencv-python-headless`, which has no GUI support at all, so these calls
  would raise immediately once the dashboard launches this script as a
  background process (no display attached). Detection, tracking, state
  logic, and Kafka publishing are unchanged — the existing
  `runtime/latest_frame.jpg` preview mechanism now serves as the video feed
  for the dashboard instead of a desktop window.
- **`requirements.txt`**: added `openpyxl`, which pandas needs as the write
  engine for the dashboard's `.xlsx` export.

A `.streamlit/config.toml` was also added at the repo root to raise
Streamlit's upload size limit for long recordings.
