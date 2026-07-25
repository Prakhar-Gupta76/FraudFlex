"""Kafka transaction producer for FraudFlux."""

from .config import KafkaProducerSettings, KafkaSecuritySettings
from .broker import BrokerStatus, inspect_broker
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
from .topics import REQUIRED_TOPICS, REQUIRED_TOPIC_NAMES, TopicSpec

__all__ = [
    "KafkaClientUnavailableError",
    "KafkaDeliveryError",
    "KafkaDeliveryTimeoutError",
    "KafkaEnqueueError",
    "KafkaPublishError",
    "KafkaProducerSettings",
    "KafkaSecuritySettings",
    "KafkaTransactionProducer",
    "BrokerStatus",
    "PublishReceipt",
    "REQUIRED_TOPICS",
    "REQUIRED_TOPIC_NAMES",
    "TopicSpec",
    "TransactionEventFactory",
    "create_confluent_producer",
    "inspect_broker",
    "serialize_transaction_event",
]
