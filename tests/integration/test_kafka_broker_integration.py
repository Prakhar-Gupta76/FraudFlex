from __future__ import annotations

import importlib.util
import json
import os
import time
import unittest
from uuid import uuid4

from fraudflux_kafka import (
    KafkaProducerSettings,
    KafkaTransactionProducer,
    REQUIRED_TOPIC_NAMES,
)
from fraudflux_simulator import TransactionSimulator

RUN_LIVE_TESTS = (
    os.getenv("FRAUDFLUX_RUN_KAFKA_INTEGRATION") == "1"
    and importlib.util.find_spec("confluent_kafka") is not None
)


@unittest.skipUnless(
    RUN_LIVE_TESTS,
    "set FRAUDFLUX_RUN_KAFKA_INTEGRATION=1 with Kafka running",
)
class KafkaBrokerIntegrationTests(unittest.TestCase):
    bootstrap_servers = os.getenv(
        "FRAUDFLUX_KAFKA_BOOTSTRAP_SERVERS",
        "127.0.0.1:9092",
    )

    @classmethod
    def setUpClass(cls) -> None:
        from confluent_kafka.admin import AdminClient

        cls.admin = AdminClient(
            {"bootstrap.servers": cls.bootstrap_servers}
        )
        metadata = cls.admin.list_topics(timeout=10)
        missing = REQUIRED_TOPIC_NAMES.difference(metadata.topics)
        if missing:
            raise AssertionError(
                f"required Kafka topics are missing: {sorted(missing)}"
            )

    def test_required_topics_and_partition_counts_exist(self) -> None:
        metadata = self.admin.list_topics(timeout=10)

        self.assertEqual(
            3, len(metadata.topics["transactions.raw"].partitions)
        )
        self.assertEqual(
            3, len(metadata.topics["transactions.scored"].partitions)
        )
        self.assertEqual(1, len(metadata.topics["fraud.alerts"].partitions))
        self.assertEqual(
            1,
            len(metadata.topics["transactions.dead-letter"].partitions),
        )

    def test_records_wait_for_a_consumer_and_keep_customer_order(self) -> None:
        from confluent_kafka import Consumer

        simulator = TransactionSimulator(seed=401)
        events = [
            generated.public_event()
            for generated in simulator.generate(
                count=5,
                scenario="account_takeover",
                rate=5,
            )
        ]
        expected_ids = [event["event_id"] for event in events]

        producer = KafkaTransactionProducer.from_settings(
            KafkaProducerSettings(
                bootstrap_servers=self.bootstrap_servers,
                delivery_timeout_ms=10_000,
                request_timeout_ms=5_000,
            )
        )
        receipts = [producer.publish_event(event) for event in events]
        producer.close()

        self.assertEqual(1, len({receipt.partition for receipt in receipts}))

        # The consumer starts only after publication, demonstrating retention
        # while a scoring worker is unavailable.
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": f"fraudflux-integration-{uuid4()}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe(["transactions.raw"])
        received_ids: list[str] = []
        deadline = time.monotonic() + 20
        try:
            while time.monotonic() < deadline:
                message = consumer.poll(1)
                if message is None:
                    continue
                if message.error():
                    self.fail(str(message.error()))
                event = json.loads(message.value().decode("utf-8"))
                if event["event_id"] in expected_ids:
                    received_ids.append(event["event_id"])
                    self.assertEqual(
                        events[0]["transaction"]["customer_id"].encode(),
                        message.key(),
                    )
                if len(received_ids) == len(expected_ids):
                    break
        finally:
            consumer.close()

        self.assertEqual(expected_ids, received_ids)


if __name__ == "__main__":
    unittest.main()
