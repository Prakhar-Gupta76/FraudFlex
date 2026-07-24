"""Shared scoring and decision-processing path for Kafka and HTTP."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from fraudflux_monitoring import OperationalMonitor
from fraudflux_validation import TransactionEvent

from .domain import (
    AnomalyEvaluation,
    CombinedRiskScore,
    FeatureSet,
    RecommendedAction,
    RiskCategory,
    RiskDecision,
    RuleEvaluation,
    StoredDecision,
)
from .outputs import DecisionOutputFactory
from .ports import (
    AnomalyModel,
    DecisionEngine,
    FeatureCalculator,
    HistoryProvider,
    OutputPublisher,
    ProcessingStore,
    RiskCombiner,
    RulesEngine,
)


@dataclass(frozen=True)
class ScoringResult:
    features: FeatureSet
    rules: RuleEvaluation
    anomaly: AnomalyEvaluation
    combined_score: CombinedRiskScore
    decision: RiskDecision


@dataclass(frozen=True)
class ProcessedDecision:
    stored: StoredDecision
    created: bool


class SharedScoringPipeline:
    """One authoritative scoring implementation for all input transports."""

    def __init__(
        self,
        *,
        history_provider: HistoryProvider,
        feature_calculator: FeatureCalculator,
        rules_engine: RulesEngine,
        anomaly_model: AnomalyModel,
        risk_combiner: RiskCombiner,
        decision_engine: DecisionEngine,
        timer_ns: Callable[[], int] = time.perf_counter_ns,
        monitor: OperationalMonitor | None = None,
    ) -> None:
        self.history_provider = history_provider
        self.feature_calculator = feature_calculator
        self.rules_engine = rules_engine
        self.anomaly_model = anomaly_model
        self.risk_combiner = risk_combiner
        self.decision_engine = decision_engine
        self.timer_ns = timer_ns
        self.monitor = monitor or OperationalMonitor()

    def score(self, event: TransactionEvent) -> ScoringResult:
        started = self.timer_ns()
        history = self.history_provider.load(event)
        features = self.feature_calculator.calculate(event, history)
        rules = self.rules_engine.evaluate(event, history, features)
        try:
            anomaly = self.anomaly_model.evaluate(features)
        except Exception:
            self.monitor.record_model_failure(
                str(getattr(self.anomaly_model, "model_version", "unknown"))
            )
            raise
        combined_score = self.risk_combiner.combine(rules, anomaly)
        upstream_latency_ms = max(
            0.0,
            (self.timer_ns() - started) / 1_000_000,
        )
        decision = self.decision_engine.decide(
            combined_score,
            rules,
            anomaly,
            upstream_processing_latency_ms=upstream_latency_ms,
        )
        validate_decision(combined_score, decision)
        self.monitor.record_scoring(
            decision.processing_latency_ms,
            rule_ids=(hit.rule_id for hit in rules.hits),
        )
        return ScoringResult(
            features=features,
            rules=rules,
            anomaly=anomaly,
            combined_score=combined_score,
            decision=decision,
        )


class DecisionProcessor:
    """Score, durably store, and publish one validated transaction."""

    def __init__(
        self,
        *,
        pipeline: SharedScoringPipeline,
        store: ProcessingStore,
        publisher: OutputPublisher,
        output_factory: DecisionOutputFactory | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        monitor: OperationalMonitor | None = None,
    ) -> None:
        self.pipeline = pipeline
        self.store = store
        self.publisher = publisher
        self.output_factory = output_factory or DecisionOutputFactory()
        self.clock = clock
        self.monitor = monitor or pipeline.monitor

    def process(self, event: TransactionEvent) -> ProcessedDecision:
        record_id = f"event:{event.event_id}"
        existing = self._database_call(
            "get_decision",
            lambda: self.store.get_decision(event.event_id),
        )
        created = False
        if existing is None:
            scored = self.pipeline.score(event)
            processed_at = self.clock()
            if processed_at.tzinfo is None:
                raise ValueError(
                    "decision processor clock must be timezone-aware"
                )
            outputs = self.output_factory.build(
                event,
                scored.rules,
                scored.anomaly,
                scored.combined_score,
                scored.decision,
                processed_at=processed_at,
            )
            candidate = StoredDecision(
                record_id=record_id,
                input_event_id=event.event_id,
                transaction_id=event.transaction.transaction_id,
                customer_id=event.transaction.customer_id,
                transaction_payload=event.model_dump(mode="json"),
                feature_values=scored.features.values,
                rules=scored.rules,
                anomaly=scored.anomaly,
                combined_score=scored.combined_score,
                decision=scored.decision,
                processed_at=processed_at.isoformat(),
            )
            created = self._database_call(
                "save_decision",
                lambda: self.store.save_decision_if_absent(
                    candidate,
                    outputs,
                ),
            )
            existing = (
                candidate
                if created
                else self._database_call(
                    "get_decision_after_conflict",
                    lambda: self.store.get_decision(event.event_id),
                )
            )
            if existing is None:
                raise RuntimeError(
                    "decision lost after a concurrent idempotency conflict"
                )

        self.publish_pending(record_id)
        return ProcessedDecision(stored=existing, created=created)

    def publish_pending(self, record_id: str) -> None:
        pending = self._database_call(
            "pending_outbox",
            lambda: self.store.pending_outbox(record_id),
        )
        for output in pending:
            try:
                self.publisher.publish(output)
            except Exception:
                self.monitor.record_publish_failure(output.topic)
                raise
            self.monitor.record_event_produced(output.topic)
            self._database_call(
                "mark_outbox_published",
                lambda: self.store.mark_outbox_published(output.outbox_id),
            )

    def _database_call(self, operation: str, callback: Callable[[], object]):
        try:
            return callback()
        except Exception:
            self.monitor.record_database_error(operation)
            raise


def validate_decision(
    combined_score: CombinedRiskScore,
    decision: RiskDecision,
) -> None:
    if decision.final_score != combined_score.final_score:
        raise ValueError(
            "decision final score does not match the combined risk score"
        )
    required = combined_score.override_action
    severity = {
        None: 0,
        RecommendedAction.APPROVE: 0,
        RecommendedAction.VERIFY: 1,
        RecommendedAction.HOLD: 2,
    }
    if severity[decision.action] < severity[required]:
        raise ValueError("decision did not honor the risk-score override")
    category_severity = {
        RiskCategory.LOW: 0,
        RiskCategory.MEDIUM: 1,
        RiskCategory.HIGH: 2,
    }
    override_category = {
        None: RiskCategory.LOW,
        RecommendedAction.VERIFY: RiskCategory.MEDIUM,
        RecommendedAction.HOLD: RiskCategory.HIGH,
    }[required]
    expected_category = max(
        (decision.score_category, override_category),
        key=category_severity.__getitem__,
    )
    if decision.category != expected_category:
        raise ValueError(
            "decision category is not justified by score or override"
        )
    expected_override_applied = (
        expected_category != decision.score_category
    )
    if decision.override_applied != expected_override_applied:
        raise ValueError("decision override status is inconsistent")
