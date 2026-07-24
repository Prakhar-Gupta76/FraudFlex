"""Kafka consumer configuration for manual-offset worker processing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from fraudflux_kafka import KafkaClientUnavailableError

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

