from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from dashboard.models import DashboardEvent

CAIRO = ZoneInfo("Africa/Cairo")

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

    def close(self) -> None:
        with self._lock:
            self._connection.close()
