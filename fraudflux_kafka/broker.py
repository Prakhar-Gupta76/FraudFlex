"""Broker readiness and required-topic inspection."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from typing import Any, Dict, Mapping, Optional, Protocol, Sequence, Tuple

from .producer import KafkaClientUnavailableError
from .topics import REQUIRED_TOPIC_NAMES


class AdminClient(Protocol):
    def list_topics(self, *, timeout: float) -> Any: ...


@dataclass(frozen=True)
class BrokerStatus:
    connected: bool
    broker_count: int
    required_topics: Tuple[str, ...]
    missing_topics: Tuple[str, ...]

    @property
    def ready(self) -> bool:
        return self.connected and not self.missing_topics

    def as_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["ready"] = self.ready
        return result


def inspect_broker(
    admin_client: AdminClient,
    *,
    timeout_seconds: float = 5.0,
) -> BrokerStatus:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    metadata = admin_client.list_topics(timeout=timeout_seconds)
    brokers: Mapping[Any, Any] = getattr(metadata, "brokers", {})
    topics: Mapping[str, Any] = getattr(metadata, "topics", {})
    missing = tuple(sorted(REQUIRED_TOPIC_NAMES.difference(topics)))
    return BrokerStatus(
        connected=bool(brokers),
        broker_count=len(brokers),
        required_topics=tuple(sorted(REQUIRED_TOPIC_NAMES)),
        missing_topics=missing,
    )


def create_confluent_admin(bootstrap_servers: str) -> AdminClient:
    try:
        from confluent_kafka.admin import AdminClient as ConfluentAdminClient
    except ImportError as exc:
        raise KafkaClientUnavailableError(
            "confluent-kafka is required for broker health checks; "
            "install the project dependencies first",
            code="kafka_client_unavailable",
            retriable=False,
            fatal=True,
            cause=exc,
        ) from exc
    return ConfluentAdminClient({"bootstrap.servers": bootstrap_servers})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check the FraudFlux Kafka broker and required topics."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
    )
    parser.add_argument("--timeout", type=float, default=5.0)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        status = inspect_broker(
            create_confluent_admin(args.bootstrap_servers),
            timeout_seconds=args.timeout,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ready": False,
                    "error": type(exc).__name__,
                    "message": str(exc),
                },
                separators=(",", ":"),
            )
        )
        return 1

    print(json.dumps(status.as_dict(), separators=(",", ":")))
    return 0 if status.ready else 1

