"""Required Kafka topic definitions shared by health checks and tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int
    retention_ms: int
    retention_bytes: int
    cleanup_policy: str = "delete"

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("topic name cannot be blank")
        if self.partitions < 1:
            raise ValueError("partitions must be positive")
        if self.replication_factor < 1:
            raise ValueError("replication_factor must be positive")
        if self.retention_ms < 1:
            raise ValueError("retention_ms must be positive")
        if self.retention_bytes < 1:
            raise ValueError("retention_bytes must be positive")

    def configs(self) -> Dict[str, str]:
        return {
            "cleanup.policy": self.cleanup_policy,
            "retention.ms": str(self.retention_ms),
            "retention.bytes": str(self.retention_bytes),
        }


REQUIRED_TOPICS: Tuple[TopicSpec, ...] = (
    TopicSpec(
        name="transactions.raw",
        partitions=3,
        replication_factor=1,
        retention_ms=86_400_000,
        retention_bytes=67_108_864,
    ),
    TopicSpec(
        name="transactions.scored",
        partitions=3,
        replication_factor=1,
        retention_ms=86_400_000,
        retention_bytes=67_108_864,
    ),
    TopicSpec(
        name="fraud.alerts",
        partitions=1,
        replication_factor=1,
        retention_ms=604_800_000,
        retention_bytes=134_217_728,
    ),
    TopicSpec(
        name="transactions.dead-letter",
        partitions=1,
        replication_factor=1,
        retention_ms=604_800_000,
        retention_bytes=134_217_728,
    ),
)

REQUIRED_TOPIC_NAMES = frozenset(topic.name for topic in REQUIRED_TOPICS)

