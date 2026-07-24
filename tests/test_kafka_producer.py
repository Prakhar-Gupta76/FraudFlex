from __future__ import annotations

import json
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from fraudflux_kafka import (
    KafkaDeliveryError,
    KafkaDeliveryTimeoutError,
    KafkaEnqueueError,
    KafkaProducerSettings,
    KafkaTransactionProducer,
    TransactionEventFactory,
)
from fraudflux_simulator import TransactionSimulator
from fraudflux_validation import (
    TransactionValidationError,
    validate_transaction_event,
)


@dataclass
class FakeMessage:
    topic_name: str = "transactions.raw"
    partition_id: int = 2
    offset_value: int = 41

    def topic(self) -> str:
        return self.topic_name

    def partition(self) -> int:
        return self.partition_id

    def offset(self) -> int:
        return self.offset_value


class FakeKafkaError:
    def __init__(
        self,
        message: str,
        *,
        code: int,
        retriable: bool,
        fatal: bool,
    ) -> None:
        self.message = message
        self.error_code = code
        self.is_retriable = retriable
        self.is_fatal = fatal

    def __str__(self) -> str:
        return self.message

    def code(self) -> int:
        return self.error_code

    def retriable(self) -> bool:
        return self.is_retriable

    def fatal(self) -> bool:
        return self.is_fatal


class FakeProducerClient:
    def __init__(
        self,
        *,
        buffer_failures: int = 0,
        enqueue_error: Optional[Exception] = None,
        delivery_error: Optional[Any] = None,
        deliver_on_poll: bool = True,
    ) -> None:
        self.buffer_failures = buffer_failures
        self.enqueue_error = enqueue_error
        self.delivery_error = delivery_error
        self.deliver_on_poll = deliver_on_poll
        self.produce_calls = 0
        self.poll_calls = 0
        self.records: list[dict[str, Any]] = []
        self.pending_callback: Optional[Callable[..., None]] = None
        self.flush_remaining = 0

    def produce(self, **kwargs: Any) -> None:
        self.produce_calls += 1
        if self.produce_calls <= self.buffer_failures:
            raise BufferError("local queue full")
        if self.enqueue_error is not None:
            raise self.enqueue_error
        self.records.append(kwargs)
        self.pending_callback = kwargs["on_delivery"]

    def poll(self, timeout: float) -> int:
        self.poll_calls += 1
        if self.deliver_on_poll and self.pending_callback is not None:
            callback = self.pending_callback
            self.pending_callback = None
            callback(self.delivery_error, FakeMessage())
            return 1
        return 0

    def flush(self, timeout: float) -> int:
        return self.flush_remaining


def generated_public_event() -> dict:
    simulator = TransactionSimulator(seed=301)
    return next(
        simulator.generate(count=1, scenario="normal", rate=1)
    ).public_event()


class KafkaProducerSettingsTests(unittest.TestCase):
    def test_reliability_configuration_enables_idempotence(self) -> None:
        settings = KafkaProducerSettings()

        config = settings.confluent_config()

        self.assertIs(True, config["enable.idempotence"])
        self.assertEqual("all", config["acks"])
        self.assertEqual(5, config["max.in.flight.requests.per.connection"])
        self.assertEqual(10_000, config["delivery.timeout.ms"])

    def test_request_timeout_cannot_exceed_delivery_timeout(self) -> None:
        with self.assertRaises(ValueError):
            KafkaProducerSettings(
                delivery_timeout_ms=1000,
                request_timeout_ms=1001,
            )


class TransactionEventFactoryTests(unittest.TestCase):
    def test_factory_adds_missing_identifiers_and_metadata(self) -> None:
        public_event = generated_public_event()
        transaction = public_event["transaction"]
        del transaction["transaction_id"]
        del transaction["transaction_time"]
        identifiers = iter(("transaction-123", "event-456"))
        event_time = datetime(2026, 2, 1, 10, 30, tzinfo=timezone.utc)
        factory = TransactionEventFactory(
            clock=lambda: event_time,
            identifier_factory=lambda: next(identifiers),
        )

        event = factory.create(transaction)

        self.assertEqual("TXN-transaction-123", event.transaction.transaction_id)
        self.assertEqual("EVT-event-456", event.event_id)
        self.assertEqual("transaction.created", event.event_type)
        self.assertEqual("1.0", event.schema_version)
        self.assertEqual(event_time, event.event_time)
        self.assertEqual(event_time, event.transaction.transaction_time)

    def test_factory_does_not_mutate_caller_transaction(self) -> None:
        transaction = generated_public_event()["transaction"]
        del transaction["transaction_id"]
        original = json.loads(json.dumps(transaction))

        TransactionEventFactory().create(transaction)

        self.assertEqual(original, transaction)


class KafkaTransactionProducerTests(unittest.TestCase):
    def test_valid_event_is_serialized_and_delivered_with_customer_key(self) -> None:
        client = FakeProducerClient()
        event = generated_public_event()
        producer = KafkaTransactionProducer(client)

        receipt = producer.publish_event(event)

        self.assertEqual(1, len(client.records))
        record = client.records[0]
        self.assertEqual(
            event["transaction"]["customer_id"].encode("utf-8"),
            record["key"],
        )
        decoded = json.loads(record["value"].decode("utf-8"))
        expected = validate_transaction_event(event).model_dump(mode="json")
        self.assertEqual(expected, decoded)
        self.assertNotIn("ground_truth", decoded)
        headers = dict(record["headers"])
        self.assertEqual(b"application/json", headers["content-type"])
        self.assertEqual(event["event_id"].encode(), headers["event-id"])
        self.assertEqual("transactions.raw", receipt.topic)
        self.assertEqual(2, receipt.partition)
        self.assertEqual(41, receipt.offset)
        self.assertGreater(receipt.serialized_size_bytes, 0)

    def test_same_customer_uses_same_message_key(self) -> None:
        client = FakeProducerClient()
        producer = KafkaTransactionProducer(client)
        first = generated_public_event()
        second = generated_public_event()
        second["event_id"] = "EVT-second"
        second["transaction"]["transaction_id"] = "TXN-second"

        producer.publish_event(first)
        producer.publish_event(second)

        self.assertEqual(client.records[0]["key"], client.records[1]["key"])

    def test_publish_transaction_adds_missing_event_metadata(self) -> None:
        client = FakeProducerClient()
        producer = KafkaTransactionProducer(client)
        transaction = generated_public_event()["transaction"]

        receipt = producer.publish_transaction(transaction)

        decoded = json.loads(client.records[0]["value"].decode("utf-8"))
        self.assertEqual("transaction.created", decoded["event_type"])
        self.assertEqual("1.0", decoded["schema_version"])
        self.assertTrue(decoded["event_id"].startswith("EVT-"))
        self.assertEqual(
            transaction["transaction_id"], receipt.transaction_id
        )

    def test_invalid_event_is_rejected_before_kafka(self) -> None:
        client = FakeProducerClient()
        producer = KafkaTransactionProducer(client)
        event = generated_public_event()
        event["transaction"]["amount_minor"] = 0

        with self.assertRaises(TransactionValidationError):
            producer.publish_event(event)

        self.assertEqual(0, client.produce_calls)

    def test_local_queue_full_is_retried_with_backoff(self) -> None:
        client = FakeProducerClient(buffer_failures=2)
        sleeps: list[float] = []
        settings = KafkaProducerSettings(
            local_queue_retry_attempts=3,
            local_queue_retry_backoff_seconds=0.01,
        )
        producer = KafkaTransactionProducer(
            client,
            settings=settings,
            sleep=sleeps.append,
        )

        producer.publish_event(generated_public_event())

        self.assertEqual(3, client.produce_calls)
        self.assertEqual([0.01, 0.02], sleeps)

    def test_exhausted_local_queue_retries_are_reported(self) -> None:
        client = FakeProducerClient(buffer_failures=3)
        settings = KafkaProducerSettings(
            local_queue_retry_attempts=2,
            local_queue_retry_backoff_seconds=0,
        )
        producer = KafkaTransactionProducer(
            client,
            settings=settings,
            sleep=lambda _: None,
        )

        with self.assertRaises(KafkaEnqueueError) as context:
            producer.publish_event(generated_public_event())

        self.assertEqual("local_queue_full", context.exception.code)
        self.assertTrue(context.exception.retriable)
        self.assertFalse(context.exception.fatal)
        self.assertEqual(3, client.produce_calls)

    def test_permanent_enqueue_failure_is_reported(self) -> None:
        error = FakeKafkaError(
            "invalid topic",
            code=17,
            retriable=False,
            fatal=True,
        )
        client = FakeProducerClient(enqueue_error=RuntimeError(error))
        producer = KafkaTransactionProducer(client)

        with self.assertRaises(KafkaEnqueueError) as context:
            producer.publish_event(generated_public_event())

        self.assertEqual("17", context.exception.code)
        self.assertFalse(context.exception.retriable)
        self.assertTrue(context.exception.fatal)

    def test_delivery_failure_is_classified(self) -> None:
        error = FakeKafkaError(
            "broker temporarily unavailable",
            code=3,
            retriable=True,
            fatal=False,
        )
        client = FakeProducerClient(delivery_error=error)
        producer = KafkaTransactionProducer(client)

        with self.assertRaises(KafkaDeliveryError) as context:
            producer.publish_event(generated_public_event())

        self.assertEqual("3", context.exception.code)
        self.assertTrue(context.exception.retriable)
        self.assertFalse(context.exception.fatal)

    def test_delivery_timeout_reports_unknown_state_without_resend(self) -> None:
        client = FakeProducerClient(deliver_on_poll=False)
        settings = KafkaProducerSettings(
            delivery_timeout_ms=1,
            request_timeout_ms=1,
            poll_interval_seconds=0.001,
        )
        clock_values = iter((0.0, 2.0))
        producer = KafkaTransactionProducer(
            client,
            settings=settings,
            clock=lambda: next(clock_values),
        )

        with self.assertRaises(KafkaDeliveryTimeoutError) as context:
            producer.publish_event(generated_public_event())

        self.assertTrue(context.exception.delivery_state_unknown)
        self.assertEqual(1, client.produce_calls)

    def test_close_reports_undelivered_messages(self) -> None:
        client = FakeProducerClient()
        client.flush_remaining = 2
        producer = KafkaTransactionProducer(client)

        with self.assertRaises(KafkaDeliveryTimeoutError) as context:
            producer.close()

        self.assertEqual("kafka_flush_timeout", context.exception.code)


if __name__ == "__main__":
    unittest.main()
