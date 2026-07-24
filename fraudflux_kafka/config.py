"""Configuration for the FraudFlux Kafka producer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class KafkaProducerSettings:
    bootstrap_servers: str = "localhost:9092"
    topic: str = "transactions.raw"
    client_id: str = "fraudflux-transaction-producer"
    delivery_timeout_ms: int = 10_000
    request_timeout_ms: int = 5_000
    retry_backoff_ms: int = 100
    poll_interval_seconds: float = 0.05
    local_queue_retry_attempts: int = 3
    local_queue_retry_backoff_seconds: float = 0.05

    def __post_init__(self) -> None:
        if not self.bootstrap_servers.strip():
            raise ValueError("bootstrap_servers cannot be blank")
        if not self.topic.strip():
            raise ValueError("topic cannot be blank")
        if not self.client_id.strip():
            raise ValueError("client_id cannot be blank")
        if self.delivery_timeout_ms < 1:
            raise ValueError("delivery_timeout_ms must be positive")
        if self.request_timeout_ms < 1:
            raise ValueError("request_timeout_ms must be positive")
        if self.request_timeout_ms > self.delivery_timeout_ms:
            raise ValueError(
                "request_timeout_ms cannot exceed delivery_timeout_ms"
            )
        if self.retry_backoff_ms < 0:
            raise ValueError("retry_backoff_ms cannot be negative")
        if self.poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if self.local_queue_retry_attempts < 0:
            raise ValueError("local_queue_retry_attempts cannot be negative")
        if self.local_queue_retry_backoff_seconds < 0:
            raise ValueError(
                "local_queue_retry_backoff_seconds cannot be negative"
            )

    @property
    def delivery_wait_timeout_seconds(self) -> float:
        # Small callback allowance after the librdkafka delivery deadline.
        return self.delivery_timeout_ms / 1000 + 1.0

    def confluent_config(self) -> Dict[str, Any]:
        """Return reliability-focused librdkafka producer configuration."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": self.delivery_timeout_ms,
            "request.timeout.ms": self.request_timeout_ms,
            "retry.backoff.ms": self.retry_backoff_ms,
            "max.in.flight.requests.per.connection": 5,
        }
