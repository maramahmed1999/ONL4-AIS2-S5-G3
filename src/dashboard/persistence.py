from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from dashboard.models import DashboardEvent, TrackSummary

CAIRO = ZoneInfo("Africa/Cairo")


@dataclass(frozen=True)
class MotionSample:

    track_id: int
    timestamp: datetime
    motion_score: float


@dataclass(frozen=True)
class ModelMetricSample:
  

    timestamp: datetime
    avg_confidence: float | None
    fps: float | None
    inference_time_ms: float | None


_SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    video_name TEXT NOT NULL,
    started_at DATETIME NOT NULL,
    ended_at DATETIME,
    total_tracked INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS excavator_summary (
    session_id TEXT NOT NULL,
    track_id INTEGER NOT NULL,
    state TEXT NOT NULL,
    working_seconds REAL NOT NULL,
    idle_seconds REAL NOT NULL,
    total_observed_seconds REAL NOT NULL,
    utilization_percent REAL NOT NULL,
    last_seen DATETIME NOT NULL,
    PRIMARY KEY (session_id, track_id),
    FOREIGN KEY (session_id) REFERENCES sessions (session_id)
);

CREATE TABLE IF NOT EXISTS excavator_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    track_id INTEGER NOT NULL,
    timestamp DATETIME NOT NULL,
    frame_id INTEGER NOT NULL,
    video_time_seconds REAL NOT NULL,
    state TEXT NOT NULL,
    motion_score REAL NOT NULL,
    working_seconds REAL NOT NULL,
    idle_seconds REAL NOT NULL,
    FOREIGN KEY (session_id) REFERENCES sessions (session_id)
);

CREATE INDEX IF NOT EXISTS idx_excavator_events_session_track
    ON excavator_events (session_id, track_id);

CREATE TABLE IF NOT EXISTS model_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    timestamp DATETIME NOT NULL,
    avg_confidence REAL,
    fps REAL,
    inference_time_ms REAL
);

CREATE INDEX IF NOT EXISTS idx_model_metrics_timestamp
    ON model_metrics (timestamp);
"""


def _iso(value: datetime) -> str:
    """Store every timestamp as Cairo local time, ISO-8601 text — matches
    the timezone already used for display everywhere else in the dashboard
    (tables.py, export.py, controls.py all render in Africa/Cairo). Naive
    datetimes are assumed UTC first (matching DashboardEvent's own
    normalization in models.py), then converted to Cairo."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(CAIRO).isoformat()


class PersistenceRepository:
    """SQLite-backed durability layer that sits alongside the existing
    in-memory EventStore — it does not replace it, and it changes nothing
    about the live pipeline: cv_service, the Kafka event schema,
    EventStore's snapshotting, and the Analytics tab all work exactly as
    they did before. This class is only ever written to; nothing in the
    dashboard reads from it (yet) — it exists purely so tracking history
    survives a dashboard/process restart.

    One connection is shared between the Kafka consumer thread (writes
    events) and the Streamlit main thread (starts/stops sessions), guarded
    by a lock, since sqlite3 connections aren't safe for concurrent access
    from multiple threads without one.
    """

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(str(db_path), check_same_thread=False)
        with self._lock:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._connection.executescript(_SCHEMA)
            self._connection.commit()

    def start_session(self, session_id: str, video_name: str, started_at: datetime) -> None:
        """Insert a new session row when the pipeline starts a run.

        INSERT OR IGNORE: PipelineManager mints a fresh uuid per run, so a
        collision would only happen on a duplicate call for the same
        run — in which case the original started_at should win, not be
        overwritten.
        """
        with self._lock:
            self._connection.execute(
                """
                INSERT OR IGNORE INTO sessions (session_id, video_name, started_at, ended_at, total_tracked)
                VALUES (?, ?, ?, NULL, 0)
                """,
                (session_id, video_name, _iso(started_at)),
            )
            self._connection.commit()

    def end_session(self, session_id: str, ended_at: datetime) -> None:
        """Stamp ended_at — called both on a user-initiated stop and when
        the pipeline process exits on its own (end of video / crash)."""
        with self._lock:
            self._connection.execute(
                "UPDATE sessions SET ended_at = ? WHERE session_id = ?",
                (_iso(ended_at), session_id),
            )
            self._connection.commit()

    def record_event(self, event: DashboardEvent, session_id: str) -> None:
        """Append one raw event row, and upsert that track's rolled-up
        excavator_summary row plus the session's distinct-track count in
        the same transaction — so excavator_summary and
        sessions.total_tracked can never drift out of sync with the raw
        excavator_events log.
        """
        total_observed = event.working_seconds + event.idle_seconds
        utilization = (event.working_seconds / total_observed * 100.0) if total_observed > 0 else 0.0

        with self._lock:
            self._connection.execute(
                """
                INSERT INTO excavator_events (
                    session_id, track_id, timestamp, frame_id,
                    video_time_seconds, state, motion_score,
                    working_seconds, idle_seconds
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    event.track_id,
                    _iso(event.timestamp),
                    event.frame_id,
                    event.video_time_seconds,
                    event.state,
                    event.motion_score,
                    event.working_seconds,
                    event.idle_seconds,
                ),
            )

            self._connection.execute(
                """
                INSERT INTO excavator_summary (
                    session_id, track_id, state, working_seconds, idle_seconds,
                    total_observed_seconds, utilization_percent, last_seen
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (session_id, track_id) DO UPDATE SET
                    state = excluded.state,
                    working_seconds = excluded.working_seconds,
                    idle_seconds = excluded.idle_seconds,
                    total_observed_seconds = excluded.total_observed_seconds,
                    utilization_percent = excluded.utilization_percent,
                    last_seen = excluded.last_seen
                """,
                (
                    session_id,
                    event.track_id,
                    event.state,
                    event.working_seconds,
                    event.idle_seconds,
                    total_observed,
                    utilization,
                    _iso(event.timestamp),
                ),
            )

            self._connection.execute(
                """
                UPDATE sessions
                SET total_tracked = (
                    SELECT COUNT(DISTINCT track_id) FROM excavator_summary WHERE session_id = ?
                )
                WHERE session_id = ?
                """,
                (session_id, session_id),
            )
            self._connection.commit()

    # ── Reads (power the Analytics tab) ─────────────────────────────────

    def list_sessions(self) -> list[str]:
        """Every session ID, oldest to newest — survives a dashboard restart,
        unlike EventStore.list_sessions()."""
        with self._lock:
            rows = self._connection.execute(
                "SELECT session_id FROM sessions ORDER BY started_at ASC"
            ).fetchall()
        return [row[0] for row in rows]

    def session_label(self, session_id: str) -> str | None:
        """Human-readable label ('Upload: clip.mp4') for a session ID, read
        back from disk — works even for sessions from a previous process,
        unlike PipelineManager's in-memory _session_labels dict."""
        with self._lock:
            row = self._connection.execute(
                "SELECT video_name FROM sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()
        return row[0] if row else None

    def fetch_summary(self, session_id: str) -> list[TrackSummary]:
        """One row per excavator tracked in this session, sorted by track ID."""
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT track_id, state, working_seconds, idle_seconds, last_seen
                FROM excavator_summary
                WHERE session_id = ?
                ORDER BY track_id ASC
                """,
                (session_id,),
            ).fetchall()
        return [
            TrackSummary(
                track_id=track_id,
                session_id=session_id,
                state=state,
                working_seconds=working_seconds,
                idle_seconds=idle_seconds,
                last_seen=datetime.fromisoformat(last_seen),
            )
            for track_id, state, working_seconds, idle_seconds, last_seen in rows
        ]

    def fetch_all_summaries(self) -> list[TrackSummary]:
        """Same as fetch_summary, merged across every session — one row per
        (session, track), sorted by session first then track ID."""
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT session_id, track_id, state, working_seconds, idle_seconds, last_seen
                FROM excavator_summary
                ORDER BY session_id ASC, track_id ASC
                """
            ).fetchall()
        return [
            TrackSummary(
                track_id=track_id,
                session_id=session_id,
                state=state,
                working_seconds=working_seconds,
                idle_seconds=idle_seconds,
                last_seen=datetime.fromisoformat(last_seen),
            )
            for session_id, track_id, state, working_seconds, idle_seconds, last_seen in rows
        ]

    def fetch_motion_history(self, session_id: str) -> list[tuple[str, MotionSample]]:
        """Every raw motion sample for one session, chronological — backs
        the motion chart the same way EventStore.history used to."""
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT track_id, timestamp, motion_score
                FROM excavator_events
                WHERE session_id = ?
                ORDER BY timestamp ASC
                """,
                (session_id,),
            ).fetchall()
        return [
            (session_id, MotionSample(track_id=track_id, timestamp=datetime.fromisoformat(ts), motion_score=motion_score))
            for track_id, ts, motion_score in rows
        ]

    def fetch_all_motion_history(self) -> list[tuple[str, MotionSample]]:
        """Same as fetch_motion_history, merged across every session."""
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT session_id, track_id, timestamp, motion_score
                FROM excavator_events
                ORDER BY timestamp ASC
                """
            ).fetchall()
        return [
            (session_id, MotionSample(track_id=track_id, timestamp=datetime.fromisoformat(ts), motion_score=motion_score))
            for session_id, track_id, ts, motion_score in rows
        ]

    def count_events(self, session_id: str) -> int:
        with self._lock:
            row = self._connection.execute(
                "SELECT COUNT(*) FROM excavator_events WHERE session_id = ?",
                (session_id,),
            ).fetchone()
        return row[0] if row else 0

    def count_all_events(self) -> int:
        with self._lock:
            row = self._connection.execute("SELECT COUNT(*) FROM excavator_events").fetchone()
        return row[0] if row else 0

    # ── Model performance (powers the System Monitoring tab) ────────────

    def record_model_metric(
        self,
        session_id: str,
        timestamp: datetime,
        avg_confidence: float | None,
        fps: float | None,
        inference_time_ms: float | None,
    ) -> None:
        """Append one rolled-up YOLO performance sample. Called periodically
        by cv_service (not per-frame) — see settings.model_metrics_interval_seconds."""
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO model_metrics (
                    session_id, timestamp, avg_confidence, fps, inference_time_ms
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (session_id, _iso(timestamp), avg_confidence, fps, inference_time_ms),
            )
            self._connection.commit()

    def latest_model_metric(self) -> ModelMetricSample | None:
        """Most recent performance sample across every session — backs the
        live KPI cards at the top of the System Monitoring tab."""
        with self._lock:
            row = self._connection.execute(
                """
                SELECT timestamp, avg_confidence, fps, inference_time_ms
                FROM model_metrics
                ORDER BY timestamp DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        ts, avg_conf, fps, inference_ms = row
        return ModelMetricSample(
            timestamp=datetime.fromisoformat(ts),
            avg_confidence=avg_conf,
            fps=fps,
            inference_time_ms=inference_ms,
        )

    def fetch_model_metrics_since(self, hours: float) -> list[ModelMetricSample]:
        """Every performance sample in the last `hours`, chronological —
        backs the confidence-trend chart (default window: last 24 hours)."""
        cutoff = _iso(datetime.now(timezone.utc) - timedelta(hours=hours))
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT timestamp, avg_confidence, fps, inference_time_ms
                FROM model_metrics
                WHERE timestamp >= ?
                ORDER BY timestamp ASC
                """,
                (cutoff,),
            ).fetchall()
        return [
            ModelMetricSample(
                timestamp=datetime.fromisoformat(ts),
                avg_confidence=avg_conf,
                fps=fps,
                inference_time_ms=inference_ms,
            )
            for ts, avg_conf, fps, inference_ms in rows
        ]

    def close(self) -> None:
        with self._lock:
            self._connection.close()
