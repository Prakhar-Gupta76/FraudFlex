"""Kafka transaction producer for FraudFlux."""

from .config import KafkaProducerSettings
from .events import TransactionEventFactory, serialize_transaction_event
from .producer import (
    KafkaClientUnavailableError,
    KafkaDeliveryError,
    KafkaDeliveryTimeoutError,
    KafkaEnqueueError,
    KafkaPublishError,
    KafkaTransactionProducer,
    PublishReceipt,
    create_confluent_producer,
)

__all__ = [
    "KafkaClientUnavailableError",
    "KafkaDeliveryError",
    "KafkaDeliveryTimeoutError",
    "KafkaEnqueueError",
    "KafkaPublishError",
    "KafkaProducerSettings",
    "KafkaTransactionProducer",
    "PublishReceipt",
    "TransactionEventFactory",
    "create_confluent_producer",
    "serialize_transaction_event",
]
