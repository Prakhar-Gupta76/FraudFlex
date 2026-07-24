"""Kafka consumer configuration for manual-offset worker processing."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict

from fraudflux_events import (
    FraudAlertEvent,
    ScoredTransactionEvent,
    parse_decision_event,
)
from fraudflux_kafka import (
    KafkaClientUnavailableError,
    KafkaProducerSettings,
    KafkaTransactionProducer,
)
from fraudflux_kafka.producer import ProducerClient
from fraudflux_validation import DeadLetterEvent

from .domain import OutboxMessage
from .ports import Consumer


@dataclass(frozen=True)
class KafkaConsumerSettings:
    bootstrap_servers: str = "localhost:9092"
    group_id: str = "fraudflux-scoring-worker"
    topic: str = "transactions.raw"
    auto_offset_reset: str = "earliest"
    poll_timeout_seconds: float = 1.0

    def __post_init__(self) -> None:
        if not self.bootstrap_servers.strip():
            raise ValueError("bootstrap_servers cannot be blank")
        if not self.group_id.strip():
            raise ValueError("group_id cannot be blank")
        if not self.topic.strip():
            raise ValueError("topic cannot be blank")
        if self.auto_offset_reset not in {"earliest", "latest", "error"}:
            raise ValueError("auto_offset_reset is invalid")
        if self.poll_timeout_seconds <= 0:
            raise ValueError("poll_timeout_seconds must be positive")

    def confluent_config(self) -> Dict[str, Any]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": self.auto_offset_reset,
            "isolation.level": "read_committed",
            "enable.partition.eof": False,
        }


def create_confluent_consumer(settings: KafkaConsumerSettings) -> Consumer:
    try:
        from confluent_kafka import Consumer as ConfluentConsumer
    except ImportError as exc:
        raise KafkaClientUnavailableError(
            "confluent-kafka is required for the scoring worker consumer",
            code="kafka_client_unavailable",
            retriable=False,
            fatal=True,
            cause=exc,
        ) from exc
    consumer = ConfluentConsumer(settings.confluent_config())
    consumer.subscribe([settings.topic])
    return consumer


@dataclass(frozen=True)
class OutputPublishReceipt:
    outbox_id: str
    event_id: str
    topic: str
    partition: int
    offset: int
    serialized_size_bytes: int


class KafkaOutputPublisher:
    """Validate and synchronously publish worker outbox messages."""

    scored_topic = "transactions.scored"
    alert_topic = "fraud.alerts"
    dead_letter_topic = "transactions.dead-letter"

    def __init__(
        self,
        client: ProducerClient,
        *,
        settings: KafkaProducerSettings | None = None,
    ) -> None:
        resolved = settings or KafkaProducerSettings(
            client_id="fraudflux-worker-output"
        )
        self._reliable_producer = KafkaTransactionProducer(
            client,
            settings=resolved,
        )

    @classmethod
    def from_settings(
        cls,
        settings: KafkaProducerSettings | None = None,
    ) -> "KafkaOutputPublisher":
        resolved = settings or KafkaProducerSettings(
            client_id="fraudflux-worker-output"
        )
        from fraudflux_kafka import create_confluent_producer

        return cls(create_confluent_producer(resolved), settings=resolved)

    def publish(self, message: OutboxMessage) -> OutputPublishReceipt:
        event_id, event_type, schema_version = self._validate(message)
        serialized = json.dumps(
            message.payload,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        headers = (
            ("content-type", b"application/json"),
            ("event-id", event_id.encode("utf-8")),
            ("event-type", event_type.encode("utf-8")),
            ("schema-version", schema_version.encode("utf-8")),
            ("outbox-id", message.outbox_id.encode("utf-8")),
        )
        delivered = self._reliable_producer.publish_serialized(
            event_id=event_id,
            topic=message.topic,
            key=message.key.encode("utf-8"),
            value=serialized,
            headers=headers,
        )
        return OutputPublishReceipt(
            outbox_id=message.outbox_id,
            event_id=event_id,
            topic=delivered.topic(),
            partition=delivered.partition(),
            offset=delivered.offset(),
            serialized_size_bytes=len(serialized),
        )

    def close(self, timeout_seconds: float = 5.0) -> None:
        self._reliable_producer.close(timeout_seconds)

    def _validate(self, message: OutboxMessage) -> tuple[str, str, str]:
        if message.topic == self.scored_topic:
            event = parse_decision_event(message.payload)
            if not isinstance(event, ScoredTransactionEvent):
                raise ValueError(
                    "transactions.scored requires a scored event"
                )
            return event.event_id, event.event_type, event.schema_version
        if message.topic == self.alert_topic:
            event = parse_decision_event(message.payload)
            if not isinstance(event, FraudAlertEvent):
                raise ValueError("fraud.alerts requires an alert event")
            return event.event_id, event.event_type, event.schema_version
        if message.topic == self.dead_letter_topic:
            event = DeadLetterEvent.model_validate(message.payload)
            return (
                event.dead_letter_id,
                event.event_type,
                event.schema_version,
            )
        raise ValueError(f"unsupported worker output topic {message.topic}")
