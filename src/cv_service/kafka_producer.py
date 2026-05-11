from __future__ import annotations

import json
import logging

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class EventProducer:
    """
    Thin wrapper around confluent-kafka Producer for structured JSON event publishing.

    Design notes:
    - Uses non-blocking poll(0) after each produce() — delivery is async.
    - Delivery failures are logged via callback, not raised (fire-and-forget pattern
      suitable for telemetry; swap for synchronous flush if you need strict guarantees).
    - track_id is used as the Kafka message key → guarantees ordering per excavator
      (all events for the same machine go to the same partition).
    """

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "linger.ms": 5,           # small batching window — reduces overhead
                "acks": "1",              # leader ack only — good balance for telemetry
                "retries": 3,
            }
        )
        logger.info(f"Kafka producer ready → topic='{topic}' @ {bootstrap_servers}")

    def send(self, event: dict) -> None:
        """
        Serialize and publish an event dict to Kafka.
        Keyed by track_id to preserve per-excavator message ordering.
        """
        self._producer.produce(
            topic=self._topic,
            key=str(event["track_id"]).encode("utf-8"),
            value=json.dumps(event, default=str).encode("utf-8"),
            callback=self._on_delivery,
        )
        self._producer.poll(0)  # non-blocking — triggers delivery callbacks

    def flush(self, timeout: float = 10.0) -> None:
        """Block until all outstanding messages are delivered. Call on shutdown."""
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning(f"Kafka flush timed out — {remaining} messages still queued")

    # ── Private ───────────────────────────────────────────────────────────────

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err:
            logger.error(f"Kafka delivery failed [topic={msg.topic()}, key={msg.key()}]: {err}")
        else:
            logger.debug(f"Delivered → {msg.topic()}[{msg.partition()}] offset={msg.offset()}")
