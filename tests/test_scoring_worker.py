from __future__ import annotations

import copy
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from fraudflux_simulator import TransactionSimulator
from fraudflux_worker import (
    AnomalyEvaluation,
    CustomerHistory,
    FeatureSet,
    FraudScoringWorker,
    InMemoryProcessingStore,
    KafkaConsumerSettings,
    ProcessingOutcome,
    RecommendedAction,
    RiskCategory,
    RiskDecision,
    RuleEvaluation,
    RuleHit,
)


@dataclass
class FakeMessage:
    payload: Any
    topic_name: str = "transactions.raw"
    partition_id: int = 1
    offset_value: int = 10
    message_error: Any = None

    def value(self) -> Any:
        return self.payload

    def topic(self) -> str:
        return self.topic_name

    def partition(self) -> int:
        return self.partition_id

    def offset(self) -> int:
        return self.offset_value

    def error(self) -> Any:
        return self.message_error


class FakeConsumer:
    def __init__(self, messages: Optional[list[Any]] = None) -> None:
        self.messages = list(messages or [])
        self.commits: list[Any] = []
        self.closed = False

    def subscribe(self, topics: Any) -> None:
        pass

    def poll(self, timeout: float) -> Any:
        return self.messages.pop(0) if self.messages else None

    def commit(self, *, message: Any, asynchronous: bool) -> None:
        self.commits.append((message, asynchronous))

    def close(self) -> None:
        self.closed = True


class RecordingPublisher:
    def __init__(self, fail_count: int = 0) -> None:
        self.fail_count = fail_count
        self.messages: list[Any] = []

    def publish(self, message: Any) -> None:
        if self.fail_count:
            self.fail_count -= 1
            raise RuntimeError("temporary publication failure")
        self.messages.append(message)


class Collaborators:
    def __init__(
        self,
        *,
        category: RiskCategory = RiskCategory.LOW,
    ) -> None:
        self.calls: list[str] = []
        self.category = category

    def load(self, customer_id: str) -> CustomerHistory:
        self.calls.append("history")
        return CustomerHistory(customer_id, {"transaction_count": 12})

    def calculate(self, event: Any, history: Any) -> FeatureSet:
        self.calls.append("features")
        return FeatureSet({"amount_ratio": 4.2, "new_device": True})

    def evaluate_rules(
        self, event: Any, history: Any, features: Any
    ) -> RuleEvaluation:
        self.calls.append("rules")
        return RuleEvaluation(
            contribution=20,
            hits=(RuleHit("AMOUNT_3X", 20, "Amount is unusual"),),
            ruleset_version="rules-v1",
        )

    def evaluate_anomaly(self, features: Any) -> AnomalyEvaluation:
        self.calls.append("anomaly")
        return AnomalyEvaluation(
            contribution=12,
            raw_score=-0.42,
            deviations=("amount_ratio",),
            model_version="model-v1",
        )

    def combine(self, rules: Any, anomaly: Any) -> RiskDecision:
        self.calls.append("combine")
        score = rules.contribution + anomaly.contribution
        actions = {
            RiskCategory.LOW: RecommendedAction.APPROVE,
            RiskCategory.MEDIUM: RecommendedAction.VERIFY,
            RiskCategory.HIGH: RecommendedAction.HOLD,
        }
        return RiskDecision(
            final_score=score,
            category=self.category,
            action=actions[self.category],
            explanation=("Amount is unusual",),
        )


class RulesAdapter:
    def __init__(self, collaborators: Collaborators) -> None:
        self.collaborators = collaborators

    def evaluate(self, event: Any, history: Any, features: Any) -> Any:
        return self.collaborators.evaluate_rules(event, history, features)


class AnomalyAdapter:
    def __init__(self, collaborators: Collaborators) -> None:
        self.collaborators = collaborators

    def evaluate(self, features: Any) -> Any:
        return self.collaborators.evaluate_anomaly(features)


def valid_event() -> dict:
    return next(
        TransactionSimulator(seed=501).generate(
            count=1,
            scenario="normal",
            rate=1,
        )
    ).public_event()


def build_worker(
    *,
    category: RiskCategory = RiskCategory.LOW,
    publisher: Optional[RecordingPublisher] = None,
    consumer: Optional[FakeConsumer] = None,
    store: Optional[InMemoryProcessingStore] = None,
) -> tuple[
    FraudScoringWorker,
    Collaborators,
    RecordingPublisher,
    FakeConsumer,
    InMemoryProcessingStore,
]:
    collaborators = Collaborators(category=category)
    resolved_publisher = publisher or RecordingPublisher()
    resolved_consumer = consumer or FakeConsumer()
    resolved_store = store or InMemoryProcessingStore()
    worker = FraudScoringWorker(
        consumer=resolved_consumer,
        history_provider=collaborators,
        feature_calculator=collaborators,
        rules_engine=RulesAdapter(collaborators),
        anomaly_model=AnomalyAdapter(collaborators),
        risk_combiner=collaborators,
        store=resolved_store,
        publisher=resolved_publisher,
        clock=lambda: datetime(2026, 3, 1, tzinfo=timezone.utc),
    )
    return (
        worker,
        collaborators,
        resolved_publisher,
        resolved_consumer,
        resolved_store,
    )


class WorkerDomainTests(unittest.TestCase):
    def test_manual_consumer_configuration_disables_auto_commit(self) -> None:
        config = KafkaConsumerSettings().confluent_config()

        self.assertIs(False, config["enable.auto.commit"])
        self.assertIs(False, config["enable.auto.offset.store"])
        self.assertEqual("read_committed", config["isolation.level"])


class FraudScoringWorkerTests(unittest.TestCase):
    def test_valid_low_risk_event_runs_pipeline_and_publishes_score(self) -> None:
        worker, collaborators, publisher, consumer, store = build_worker()
        message = FakeMessage(valid_event())

        outcome = worker.process_message(message)

        self.assertEqual(ProcessingOutcome.PROCESSED, outcome)
        self.assertEqual(
            ["history", "features", "rules", "anomaly", "combine"],
            collaborators.calls,
        )
        self.assertEqual(1, store.decision_count)
        self.assertEqual(1, len(publisher.messages))
        self.assertEqual("transactions.scored", publisher.messages[0].topic)
        self.assertEqual(1, len(consumer.commits))
        self.assertFalse(consumer.commits[0][1])

    def test_medium_and_high_risk_events_create_alerts(self) -> None:
        for category in (RiskCategory.MEDIUM, RiskCategory.HIGH):
            with self.subTest(category=category):
                worker, _, publisher, _, _ = build_worker(category=category)

                worker.process_message(FakeMessage(valid_event()))

                self.assertEqual(
                    ["transactions.scored", "fraud.alerts"],
                    [message.topic for message in publisher.messages],
                )

    def test_redelivery_does_not_recalculate_or_republish(self) -> None:
        worker, collaborators, publisher, consumer, store = build_worker(
            category=RiskCategory.HIGH
        )
        event = valid_event()
        first = FakeMessage(event)
        redelivery = FakeMessage(copy.deepcopy(event))

        first_outcome = worker.process_message(first)
        second_outcome = worker.process_message(redelivery)

        self.assertEqual(ProcessingOutcome.PROCESSED, first_outcome)
        self.assertEqual(ProcessingOutcome.DUPLICATE, second_outcome)
        self.assertEqual(1, collaborators.calls.count("history"))
        self.assertEqual(1, store.decision_count)
        self.assertEqual(2, len(publisher.messages))
        self.assertEqual(2, len(consumer.commits))

    def test_publish_failure_leaves_offset_uncommitted_and_outbox_pending(
        self,
    ) -> None:
        publisher = RecordingPublisher(fail_count=1)
        worker, collaborators, publisher, consumer, store = build_worker(
            publisher=publisher,
            category=RiskCategory.HIGH,
        )
        event = valid_event()

        with self.assertRaises(RuntimeError):
            worker.process_message(FakeMessage(event))

        self.assertEqual(0, len(consumer.commits))
        self.assertEqual(1, store.decision_count)
        self.assertEqual(2, len(store.pending_outbox(f"event:{event['event_id']}")))

        outcome = worker.process_message(FakeMessage(copy.deepcopy(event)))

        self.assertEqual(ProcessingOutcome.DUPLICATE, outcome)
        self.assertEqual(1, collaborators.calls.count("history"))
        self.assertEqual(2, len(publisher.messages))
        self.assertEqual(1, len(consumer.commits))

    def test_invalid_event_is_dead_lettered_and_committed(self) -> None:
        worker, collaborators, publisher, consumer, store = build_worker()
        event = valid_event()
        event["transaction"]["amount_minor"] = 0

        outcome = worker.process_message(FakeMessage(event))

        self.assertEqual(ProcessingOutcome.REJECTED, outcome)
        self.assertEqual([], collaborators.calls)
        self.assertEqual(1, store.rejection_count)
        self.assertEqual(1, len(publisher.messages))
        self.assertEqual(
            "transactions.dead-letter", publisher.messages[0].topic
        )
        self.assertEqual(
            "transaction.validation_failed",
            publisher.messages[0].payload["event_type"],
        )
        self.assertEqual(1, len(consumer.commits))

    def test_invalid_redelivery_does_not_duplicate_dead_letter(self) -> None:
        worker, _, publisher, consumer, _ = build_worker()
        event = valid_event()
        event["transaction"]["amount_minor"] = 0
        message = FakeMessage(event)

        first = worker.process_message(message)
        second = worker.process_message(message)

        self.assertEqual(ProcessingOutcome.REJECTED, first)
        self.assertEqual(ProcessingOutcome.DUPLICATE, second)
        self.assertEqual(1, len(publisher.messages))
        self.assertEqual(2, len(consumer.commits))

    def test_scoring_failure_does_not_commit_offset(self) -> None:
        worker, collaborators, _, consumer, store = build_worker()

        def fail(_event: Any, _history: Any) -> Any:
            raise RuntimeError("feature service unavailable")

        collaborators.calculate = fail  # type: ignore[method-assign]

        with self.assertRaises(RuntimeError):
            worker.process_message(FakeMessage(valid_event()))

        self.assertEqual(0, len(consumer.commits))
        self.assertEqual(0, store.decision_count)

    def test_run_once_reports_no_message(self) -> None:
        worker, _, _, _, _ = build_worker()

        self.assertEqual(ProcessingOutcome.NO_MESSAGE, worker.run_once())

    def test_output_event_ids_are_deterministic(self) -> None:
        worker, _, publisher, _, _ = build_worker(
            category=RiskCategory.HIGH
        )
        event = valid_event()

        worker.process_message(FakeMessage(event))

        self.assertEqual(
            f"SCORED-{event['event_id']}",
            publisher.messages[0].payload["event_id"],
        )
        self.assertEqual(
            f"ALERT-{event['event_id']}",
            publisher.messages[1].payload["event_id"],
        )


if __name__ == "__main__":
    unittest.main()
