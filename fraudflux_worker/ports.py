"""Dependency contracts used by the scoring worker."""

from __future__ import annotations

from typing import Any, Optional, Protocol, Sequence

from fraudflux_validation import TransactionEvent

from .domain import (
    AnomalyEvaluation,
    CustomerHistory,
    FeatureSet,
    OutboxMessage,
    RiskDecision,
    RuleEvaluation,
    StoredDecision,
)


class ConsumedMessage(Protocol):
    def value(self) -> Any: ...

    def topic(self) -> str: ...

    def partition(self) -> int: ...

    def offset(self) -> int: ...

    def error(self) -> Any: ...


class Consumer(Protocol):
    def subscribe(self, topics: Sequence[str]) -> None: ...

    def poll(self, timeout: float) -> Optional[ConsumedMessage]: ...

    def commit(
        self,
        *,
        message: ConsumedMessage,
        asynchronous: bool,
    ) -> Any: ...

    def close(self) -> None: ...


class HistoryProvider(Protocol):
    def load(self, event: TransactionEvent) -> CustomerHistory: ...


class FeatureCalculator(Protocol):
    def calculate(
        self,
        event: TransactionEvent,
        history: CustomerHistory,
    ) -> FeatureSet: ...


class RulesEngine(Protocol):
    def evaluate(
        self,
        event: TransactionEvent,
        history: CustomerHistory,
        features: FeatureSet,
    ) -> RuleEvaluation: ...


class AnomalyModel(Protocol):
    def evaluate(self, features: FeatureSet) -> AnomalyEvaluation: ...


class RiskCombiner(Protocol):
    def combine(
        self,
        rules: RuleEvaluation,
        anomaly: AnomalyEvaluation,
    ) -> RiskDecision: ...


class ProcessingStore(Protocol):
    def get_decision(self, event_id: str) -> Optional[StoredDecision]: ...

    def save_decision_if_absent(
        self,
        decision: StoredDecision,
        outbox: Sequence[OutboxMessage],
    ) -> bool: ...

    def save_rejection_if_absent(
        self,
        record_id: str,
        outbox: Sequence[OutboxMessage],
    ) -> bool: ...

    def pending_outbox(self, record_id: str) -> Sequence[OutboxMessage]: ...

    def mark_outbox_published(self, outbox_id: str) -> None: ...


class OutputPublisher(Protocol):
    def publish(self, message: OutboxMessage) -> Any: ...
