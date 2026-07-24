"""Reliable synchronous wrapper around a Kafka producer client."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Any,
    Callable,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

from fraudflux_validation import TransactionEvent, validate_transaction_event

from .config import KafkaProducerSettings
from .events import TransactionEventFactory, serialize_transaction_event

KafkaHeaders = Sequence[Tuple[str, bytes]]


class DeliveredMessage(Protocol):
    def topic(self) -> str: ...

    def partition(self) -> int: ...

    def offset(self) -> int: ...


class ProducerClient(Protocol):
    def produce(
        self,
        *,
        topic: str,
        key: bytes,
        value: bytes,
        headers: KafkaHeaders,
        on_delivery: Callable[[Optional[Any], DeliveredMessage], None],
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


@dataclass(frozen=True)
class PublishReceipt:
    event_id: str
    transaction_id: str
    customer_id: str
    topic: str
    partition: int
    offset: int
    serialized_size_bytes: int


class KafkaPublishError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: str,
        retriable: bool,
        fatal: bool,
        delivery_state_unknown: bool = False,
        cause: Optional[BaseException] = None,
    ) -> None:
        self.code = code
        self.retriable = retriable
        self.fatal = fatal
        self.delivery_state_unknown = delivery_state_unknown
        self.cause = cause
        super().__init__(message)


class KafkaClientUnavailableError(KafkaPublishError):
    pass


class KafkaEnqueueError(KafkaPublishError):
    pass


class KafkaDeliveryError(KafkaPublishError):
    pass


class KafkaDeliveryTimeoutError(KafkaPublishError):
    pass


def create_confluent_producer(settings: KafkaProducerSettings) -> ProducerClient:
    """Create the real client, with a clear error if dependency is absent."""
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise KafkaClientUnavailableError(
            "confluent-kafka is required for the real Kafka producer; "
            "install the project dependencies first",
            code="kafka_client_unavailable",
            retriable=False,
            fatal=True,
            cause=exc,
        ) from exc
    return Producer(settings.confluent_config())


class KafkaTransactionProducer:
    """Validate, serialize, and synchronously confirm transaction delivery."""

    def __init__(
        self,
        client: ProducerClient,
        *,
        settings: Optional[KafkaProducerSettings] = None,
        event_factory: Optional[TransactionEventFactory] = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.client = client
        self.settings = settings or KafkaProducerSettings()
        self.event_factory = event_factory or TransactionEventFactory()
        self._clock = clock
        self._sleep = sleep

    @classmethod
    def from_settings(
        cls,
        settings: Optional[KafkaProducerSettings] = None,
    ) -> "KafkaTransactionProducer":
        resolved = settings or KafkaProducerSettings()
        return cls(create_confluent_producer(resolved), settings=resolved)

    def publish_transaction(
        self,
        transaction: Mapping[str, Any],
        *,
        event_time: Optional[datetime] = None,
    ) -> PublishReceipt:
        event = self.event_factory.create(transaction, event_time=event_time)
        return self.publish_validated(event)

    def publish_event(self, payload: Any) -> PublishReceipt:
        event = validate_transaction_event(payload)
        return self.publish_validated(event)

    def publish_validated(self, event: TransactionEvent) -> PublishReceipt:
        serialized = serialize_transaction_event(event)
        customer_id = event.transaction.customer_id
        key = customer_id.encode("utf-8")
        headers: List[Tuple[str, bytes]] = [
            ("content-type", b"application/json"),
            ("event-id", event.event_id.encode("utf-8")),
            ("event-type", event.event_type.encode("utf-8")),
            ("schema-version", event.schema_version.encode("utf-8")),
        ]

        completed = threading.Event()
        delivery: dict[str, Any] = {}

        def on_delivery(
            error: Optional[Any], message: DeliveredMessage
        ) -> None:
            delivery["error"] = error
            delivery["message"] = message
            completed.set()

        self._enqueue_with_retry(
            key=key,
            value=serialized,
            headers=headers,
            on_delivery=on_delivery,
        )
        self._wait_for_delivery(completed)

        delivery_error = delivery.get("error")
        if delivery_error is not None:
            code, retriable, fatal = _error_metadata(delivery_error)
            raise KafkaDeliveryError(
                f"Kafka permanently failed to deliver event "
                f"{event.event_id}: {delivery_error}",
                code=code,
                retriable=retriable,
                fatal=fatal,
                cause=delivery_error,
            )

        message = delivery["message"]
        return PublishReceipt(
            event_id=event.event_id,
            transaction_id=event.transaction.transaction_id,
            customer_id=customer_id,
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            serialized_size_bytes=len(serialized),
        )

    def close(self, timeout_seconds: float = 5.0) -> None:
        remaining = self.client.flush(timeout_seconds)
        if remaining:
            raise KafkaDeliveryTimeoutError(
                f"{remaining} Kafka message(s) remained undelivered at shutdown",
                code="kafka_flush_timeout",
                retriable=True,
                fatal=False,
                delivery_state_unknown=True,
            )

    def _enqueue_with_retry(
        self,
        *,
        key: bytes,
        value: bytes,
        headers: KafkaHeaders,
        on_delivery: Callable[[Optional[Any], DeliveredMessage], None],
    ) -> None:
        retries_used = 0
        while True:
            try:
                self.client.produce(
                    topic=self.settings.topic,
                    key=key,
                    value=value,
                    headers=headers,
                    on_delivery=on_delivery,
                )
                return
            except BufferError as exc:
                if retries_used >= self.settings.local_queue_retry_attempts:
                    raise KafkaEnqueueError(
                        "Kafka local producer queue remained full after "
                        f"{retries_used} retry attempt(s)",
                        code="local_queue_full",
                        retriable=True,
                        fatal=False,
                        cause=exc,
                    ) from exc
                self.client.poll(0)
                backoff = (
                    self.settings.local_queue_retry_backoff_seconds
                    * (2**retries_used)
                )
                self._sleep(backoff)
                retries_used += 1
            except Exception as exc:
                code, retriable, fatal = _error_metadata(exc)
                raise KafkaEnqueueError(
                    f"Kafka rejected the event before enqueue: {exc}",
                    code=code,
                    retriable=retriable,
                    fatal=fatal,
                    cause=exc,
                ) from exc

    def _wait_for_delivery(self, completed: threading.Event) -> None:
        deadline = self._clock() + self.settings.delivery_wait_timeout_seconds
        while not completed.is_set():
            remaining = deadline - self._clock()
            if remaining <= 0:
                raise KafkaDeliveryTimeoutError(
                    "Kafka delivery confirmation timed out; delivery state "
                    "is unknown and the event was not manually resent",
                    code="delivery_confirmation_timeout",
                    retriable=True,
                    fatal=False,
                    delivery_state_unknown=True,
                )
            self.client.poll(
                min(self.settings.poll_interval_seconds, remaining)
            )


def _error_metadata(error: Any) -> Tuple[str, bool, bool]:
    candidate = error
    if isinstance(error, BaseException) and error.args:
        nested = error.args[0]
        if any(
            callable(getattr(nested, attribute, None))
            for attribute in ("code", "retriable", "fatal")
        ):
            candidate = nested

    code_value = _safe_method(candidate, "code", default=None)
    code = str(code_value) if code_value is not None else type(error).__name__
    retriable = bool(_safe_method(candidate, "retriable", default=False))
    fatal = bool(_safe_method(candidate, "fatal", default=not retriable))
    return code, retriable, fatal


def _safe_method(candidate: Any, name: str, *, default: Any) -> Any:
    method = getattr(candidate, name, None)
    if not callable(method):
        return default
    try:
        return method()
    except Exception:
        return default

