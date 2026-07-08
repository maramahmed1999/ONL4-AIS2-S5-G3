from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from confluent_kafka import Consumer, KafkaError
from pydantic import ValidationError

from dashboard.models import DashboardEvent
from dashboard.persistence import PersistenceRepository
from dashboard.store import EventStore

logger = logging.getLogger(__name__)
_UNSET = object()


@dataclass(frozen=True)
class ConsumerSnapshot:
    running: bool
    last_message_at: datetime | None
    last_error: str | None
    invalid_messages: int


class KafkaDashboardConsumer:
    """Consumes Kafka telemetry in one daemon thread and updates an EventStore."""

    def __init__(
        self,
        store: EventStore,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        session_id_provider: Callable[[], str | None] | None = None,
        persistence: PersistenceRepository | None = None,
    ) -> None:
        self._store = store
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        # Asked once per incoming message: "what session/run is currently
        # active?" This is how each event gets tagged with the upload/live
        # run it belongs to, without cv_service or the Kafka schema ever
        # needing to know sessions exist. Defaults to "no session" so this
        # class still works standalone (e.g. in tests) without a provider.
        self._session_id_provider = session_id_provider or (lambda: None)
        # Optional SQLite durability layer — purely additive. When absent
        # (e.g. in tests), the consumer behaves exactly as it did before.
        self._persistence = persistence
        self._shutdown = threading.Event()
        self._status_lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._running = False
        self._last_message_at: datetime | None = None
        self._last_error: str | None = None
        self._invalid_messages = 0

    def start(self) -> None:
        with self._status_lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._shutdown.clear()
            self._thread = threading.Thread(
                target=self._run,
                daemon=True,
                name="dashboard-kafka-consumer",
            )
            self._thread.start()

    def stop(self, timeout: float = 3.0) -> None:
        self._shutdown.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)

    def snapshot(self) -> ConsumerSnapshot:
        with self._status_lock:
            return ConsumerSnapshot(
                running=self._running,
                last_message_at=self._last_message_at,
                last_error=self._last_error,
                invalid_messages=self._invalid_messages,
            )

    def _run(self) -> None:
        consumer: Consumer | None = None
        try:
            consumer = Consumer(
                {
                    "bootstrap.servers": self._bootstrap_servers,
                    "group.id": self._group_id,
                    "auto.offset.reset": "latest",
                    "enable.auto.commit": True,
                }
            )
            consumer.subscribe([self._topic])
            self._update_status(running=True, last_error=None)

            while not self._shutdown.is_set():
                message = consumer.poll(timeout=0.5)
                if message is None:
                    continue

                error = message.error()
                if error:
                    if error.code() != KafkaError._PARTITION_EOF:
                        self._update_status(last_error=str(error))
                        logger.warning("Kafka dashboard consumer error: %s", error)
                    continue

                self._handle_message(message.value())
        except Exception as exc:
            logger.exception("Dashboard Kafka consumer stopped unexpectedly")
            self._update_status(last_error=str(exc))
        finally:
            if consumer is not None:
                consumer.close()
            self._update_status(running=False)

    def _handle_message(self, raw_value: bytes | None) -> None:
        try:
            if raw_value is None:
                raise ValueError("Kafka message has no value")
            payload = json.loads(raw_value.decode("utf-8"))
            event = DashboardEvent.model_validate(payload)
        except (UnicodeDecodeError, json.JSONDecodeError, ValidationError, ValueError) as exc:
            with self._status_lock:
                self._invalid_messages += 1
                self._last_error = f"Invalid event: {exc}"
            logger.warning("Ignoring invalid dashboard event: %s", exc)
            return

        received_at = datetime.now(timezone.utc)
        # Read the current session once and reuse it for both writes below,
        # so the in-memory store and the SQLite log can never disagree
        # about which session this event belongs to (the provider's answer
        # could otherwise change between two separate reads if a run
        # stops/starts mid-message).
        session_id = self._session_id_provider()
        self._store.append(
            event,
            session_id=session_id,
            received_at=received_at,
        )
        if self._persistence is not None:
            self._persistence.record_event(event, session_id=session_id or "unknown")
        self._update_status(last_message_at=received_at, last_error=None)

    def _update_status(
        self,
        *,
        running: bool | None = None,
        last_message_at: datetime | None = None,
        last_error: str | None | object = _UNSET,
    ) -> None:
        with self._status_lock:
            if running is not None:
                self._running = running
            if last_message_at is not None:
                self._last_message_at = last_message_at
            if last_error is not _UNSET:
                self._last_error = last_error
