from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace

from fraudflux_kafka import (
    REQUIRED_TOPICS,
    REQUIRED_TOPIC_NAMES,
    inspect_broker,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class TopicSpecificationTests(unittest.TestCase):
    def test_required_topic_names_are_exact(self) -> None:
        self.assertEqual(
            {
                "transactions.raw",
                "transactions.scored",
                "fraud.alerts",
                "transactions.dead-letter",
            },
            set(REQUIRED_TOPIC_NAMES),
        )

    def test_stream_topics_have_three_partitions(self) -> None:
        specs = {topic.name: topic for topic in REQUIRED_TOPICS}

        self.assertEqual(3, specs["transactions.raw"].partitions)
        self.assertEqual(3, specs["transactions.scored"].partitions)
        self.assertEqual(1, specs["fraud.alerts"].partitions)
        self.assertEqual(1, specs["transactions.dead-letter"].partitions)

    def test_single_broker_replication_and_retention_are_explicit(self) -> None:
        for topic in REQUIRED_TOPICS:
            with self.subTest(topic=topic.name):
                self.assertEqual(1, topic.replication_factor)
                self.assertGreater(topic.retention_ms, 0)
                self.assertGreater(topic.retention_bytes, 0)
                self.assertEqual("delete", topic.cleanup_policy)


class BrokerStatusTests(unittest.TestCase):
    def test_ready_when_broker_and_all_topics_exist(self) -> None:
        metadata = SimpleNamespace(
            brokers={1: object()},
            topics={name: object() for name in REQUIRED_TOPIC_NAMES},
        )
        admin = SimpleNamespace(list_topics=lambda timeout: metadata)

        status = inspect_broker(admin)

        self.assertTrue(status.ready)
        self.assertEqual(1, status.broker_count)
        self.assertEqual((), status.missing_topics)

    def test_missing_topics_are_reported(self) -> None:
        metadata = SimpleNamespace(
            brokers={1: object()},
            topics={"transactions.raw": object()},
        )
        admin = SimpleNamespace(list_topics=lambda timeout: metadata)

        status = inspect_broker(admin)

        self.assertFalse(status.ready)
        self.assertIn("fraud.alerts", status.missing_topics)


class DockerConfigurationTests(unittest.TestCase):
    def test_compose_uses_pinned_single_node_kraft_image(self) -> None:
        compose = (REPOSITORY_ROOT / "compose.yaml").read_text(
            encoding="utf-8"
        )

        self.assertIn("apache/kafka:4.3.1", compose)
        self.assertIn('KAFKA_PROCESS_ROLES: "broker,controller"', compose)
        self.assertIn("KAFKA_CONTROLLER_QUORUM_VOTERS", compose)
        self.assertIn('KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"', compose)
        self.assertIn(
            "127.0.0.1:${FRAUDFLUX_KAFKA_PORT:-9092}:9092",
            compose,
        )
        self.assertIn('KAFKA_HEAP_OPTS: "-Xms256m -Xmx512m"', compose)
        self.assertIn(
            "kafka-data:/var/lib/kafka/data",
            compose,
        )
        self.assertIn('KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS: "3"', compose)
        self.assertIn(
            'KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS: "3"',
            compose,
        )
        self.assertIn(
            'KAFKA_SHARE_COORDINATOR_STATE_TOPIC_NUM_PARTITIONS: "3"',
            compose,
        )
        self.assertIn("mem_limit: 1g", compose)
        self.assertNotIn("zookeeper", compose.lower())

    def test_topic_bootstrap_script_contains_every_required_topic(self) -> None:
        script = (
            REPOSITORY_ROOT / "infra" / "kafka" / "create-topics.sh"
        ).read_text(encoding="utf-8")

        for topic in REQUIRED_TOPICS:
            with self.subTest(topic=topic.name):
                self.assertIn(f'"{topic.name}"', script)
                self.assertIn(
                    f'"retention.ms={topic.retention_ms}"',
                    script,
                )
                self.assertIn(
                    f'"retention.bytes={topic.retention_bytes}"',
                    script,
                )


if __name__ == "__main__":
    unittest.main()
