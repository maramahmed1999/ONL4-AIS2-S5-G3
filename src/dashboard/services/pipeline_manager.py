from __future__ import annotations

import logging
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO

from dashboard.models import PipelineSource, PipelineStatus
from dashboard.persistence import PersistenceRepository

logger = logging.getLogger(__name__)

_GRACEFUL_STOP_TIMEOUT_SECONDS = 8.0


@dataclass
class _RunningProcess:
    popen: subprocess.Popen
    source_label: str
    started_at: datetime
    log_file: BinaryIO
    session_id: str


class PipelineManager:
    """
    Owns the lifecycle of the cv_service CV pipeline as a background subprocess.

    Design notes:
    - The pipeline itself (cv_service/main.py) is never modified in behavior —
      this class only launches `python cv_service/main.py <source>` and manages
      the OS process around it.
    - Only one pipeline run is allowed at a time; start() is a no-op (raises)
      if one is already running.
    - Uploaded videos are streamed to disk in chunks (never fully buffered as
      a second in-memory copy), then cv_service reads them frame-by-frame via
      its existing cv2.VideoCapture-based CaptureThread — unchanged, so very
      long recordings are handled the same way regardless of who launches it.
    - Live camera uses a numeric device index (e.g. "0") passed as the video
      source argument; OpenCV resolves an all-digit source string to a webcam
      index on the host running the pipeline process.
    - Stopping sends SIGINT first (the same signal main.py already handles via
      `except KeyboardInterrupt`, which flushes Kafka and releases the capture
      cleanly), only escalating to SIGTERM/SIGKILL if the process doesn't exit
      in time.
    """

    def __init__(
        self,
        src_root: Path,
        uploads_dir: Path,
        log_path: Path,
        persistence: PersistenceRepository | None = None,
    ) -> None:
        self._src_root = src_root
        self._main_script = src_root / "cv_service" / "main.py"
        self._uploads_dir = uploads_dir
        self._log_path = log_path
        self._uploads_dir.mkdir(parents=True, exist_ok=True)
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        # Optional SQLite durability layer — purely additive. When absent
        # (e.g. in tests), PipelineManager behaves exactly as it did before.
        self._persistence = persistence

        self._lock = threading.RLock()
        self._process: _RunningProcess | None = None
        self._last_error: str | None = None
        self._last_return_code: int | None = None
        # The session ID of the currently-running (or most recently started)
        # pipeline run, and a lookup of every session's human-readable label
        # ("Upload: clip.mp4", "Live Camera (device 0)") for the UI picker.
        self._current_session_id: str | None = None
        self._session_labels: dict[str, str] = {}

    # ── Public API ────────────────────────────────────────────────────────

    def save_upload(self, uploaded_file, filename: str) -> Path:
        """Streams a Streamlit UploadedFile to disk in chunks and returns its path."""
        safe_name = f"{int(time.time())}_{Path(filename).name}"
        destination = self._uploads_dir / safe_name
        uploaded_file.seek(0)
        with destination.open("wb") as out_file:
            shutil.copyfileobj(uploaded_file, out_file, length=4 * 1024 * 1024)
        return destination
    
    def start(self, source: PipelineSource, video_path: Path | None, camera_index: int | None) -> None:
        with self._lock:
            if self.is_running():
                raise RuntimeError("Detection is already running — stop it before starting a new run.")

            # Mint a new session ID for this run only once we know it's
            # actually going to start — this is what keeps this run's
            # excavator track IDs from being merged with the previous run's.
            session_id = uuid.uuid4().hex[:8]

            if source is PipelineSource.UPLOAD:
                if video_path is None or not video_path.exists():
                    raise ValueError("No uploaded video file found to process.")
                source_arg = str(video_path)
                source_label = f"Upload: {video_path.name}"
            else:
                index = 0 if camera_index is None else camera_index
                source_arg = str(index)
                source_label = f"Live Camera (device {index})"

            command = [sys.executable, str(self._main_script), source_arg]

            log_file = self._log_path.open("ab", buffering=0)
            log_file.write(
                f"\n\n===== Pipeline start {datetime.now(timezone.utc).isoformat()} — {source_label} =====\n".encode()
            )

            popen = subprocess.Popen(
                command,
                cwd=str(self._src_root),
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=(os.name == "posix"),
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name != "posix" else 0
            )

            self._process = _RunningProcess(
                popen=popen,
                source_label=source_label,
                started_at=datetime.now(timezone.utc),
                log_file=log_file,
                session_id=session_id,
            )
            self._current_session_id = session_id
            self._session_labels[session_id] = source_label
            self._last_error = None
            self._last_return_code = None
            if self._persistence is not None:
                self._persistence.start_session(
                    session_id,
                    video_name=source_label,
                    started_at=self._process.started_at,
                )
            logger.info("Pipeline started (pid=%s, session=%s): %s", popen.pid, session_id, source_label)

    def stop(self) -> None:
        with self._lock:
            process = self._process
            if process is None:
                return

            popen = process.popen
            if popen.poll() is None:
                self._send_signal(popen, self._interrupt_signal())
                try:
                    popen.wait(timeout=_GRACEFUL_STOP_TIMEOUT_SECONDS)
                except subprocess.TimeoutExpired:
                    logger.warning("Pipeline did not stop gracefully — terminating (pid=%s)", popen.pid)
                    popen.terminate()
                    try:
                        popen.wait(timeout=3.0)
                    except subprocess.TimeoutExpired:
                        popen.kill()
                        popen.wait(timeout=3.0)

            self._last_return_code = popen.returncode
            process.log_file.close()
            self._process = None
            if self._persistence is not None:
                self._persistence.end_session(process.session_id, ended_at=datetime.now(timezone.utc))
            logger.info("Pipeline stopped (return_code=%s)", self._last_return_code)

    def is_running(self) -> bool:
        with self._lock:
            if self._process is None:
                return False
            if self._process.popen.poll() is None:
                return True
            # Process exited on its own (end of video, or crashed).
            self._last_return_code = self._process.popen.returncode
            if self._last_return_code not in (0, None):
                self._last_error = (
                    f"Pipeline process exited with code {self._last_return_code}. "
                    "Check the pipeline log for details."
                )
            self._process.log_file.close()
            if self._persistence is not None:
                self._persistence.end_session(self._process.session_id, ended_at=datetime.now(timezone.utc))
            self._process = None
            return False

    def status(self) -> PipelineStatus:
        with self._lock:
            running = self.is_running()
            if running and self._process is not None:
                return PipelineStatus(
                    running=True,
                    pid=self._process.popen.pid,
                    source_label=self._process.source_label,
                    started_at=self._process.started_at,
                    return_code=None,
                    error=None,
                    session_id=self._process.session_id,
                )
            return PipelineStatus(
                running=False,
                pid=None,
                source_label=None,
                started_at=None,
                return_code=self._last_return_code,
                error=self._last_error,
                session_id=self._current_session_id,
            )

    def current_session_id(self) -> str | None:
        """The session ID of the currently-running (or most recently started) run.

        Used by KafkaDashboardConsumer to stamp every incoming event with the
        session it belongs to, and by the Live tab to always show the
        current run's data.
        """
        with self._lock:
            return self._current_session_id

    def session_label(self, session_id: str) -> str | None:
        """Human-readable label for a session ID, e.g. 'Upload: clip.mp4'."""
        with self._lock:
            return self._session_labels.get(session_id)

    def tail_log(self, max_lines: int = 200) -> str:
        if not self._log_path.exists():
            return ""
        with self._log_path.open("r", encoding="utf-8", errors="replace") as log_file:
            lines = log_file.readlines()
        return "".join(lines[-max_lines:])

    # ── Private helpers ──────────────────────────────────────────────────

    @staticmethod
    def _interrupt_signal() -> signal.Signals:
        return signal.SIGINT if os.name == "posix" else signal.CTRL_BREAK_EVENT

    @staticmethod
    def _send_signal(popen: subprocess.Popen, sig: signal.Signals) -> None:
        try:
            popen.send_signal(sig)
        except (ProcessLookupError, OSError):
            pass
