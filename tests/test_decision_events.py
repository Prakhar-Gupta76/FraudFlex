from __future__ import annotations

import copy
import json
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from pydantic import ValidationError

from fraudflux_events import (
    FraudAlertEvent,
    ScoredTransactionEvent,
    parse_decision_event,
)
from fraudflux_simulator import TransactionSimulator
from fraudflux_validation import validate_transaction_event
from fraudflux_worker import (
    AnomalyEvaluation,
    CombinedRiskScore,
    KafkaOutputPublisher,
    OutboxMessage,
    RecommendedAction,
    RiskCategory,
    RiskDecision,
    RuleEvaluation,
    RuleHit,
)
from fraudflux_worker.outputs import DecisionOutputFactory


@dataclass
class DeliveredMessage:
    topic_name: str
    partition_id: int = 1
    offset_value: int = 9

    def topic(self) -> str:
        return self.topic_name

    def partition(self) -> int:
        return self.partition_id

    def offset(self) -> int:
        return self.offset_value


class FakeProducerClient:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []
        self.pending: Optional[Callable[..., None]] = None

    def produce(self, **kwargs: Any) -> None:
        self.records.append(kwargs)
        self.pending = kwargs["on_delivery"]

    def poll(self, timeout: float) -> int:
        if self.pending is None:
            return 0
        callback = self.pending
        self.pending = None
        callback(None, DeliveredMessage(self.records[-1]["topic"]))
        return 1

    def flush(self, timeout: float) -> int:
        return 0


def make_outputs(
    *,
    category: RiskCategory = RiskCategory.LOW,
) -> list:
    event = validate_transaction_event(
        next(
            TransactionSimulator(seed=1201).generate(
                count=1,
                scenario="normal",
                rate=1,
            )
        ).public_event()
    )
    rules = RuleEvaluation(
        contribution=20,
        hits=(RuleHit("AMOUNT_3X", 20, "Observed elevated amount."),),
        ruleset_version="rules-v1",
        override_action=(
            None
            if category == RiskCategory.LOW
            else (
                RecommendedAction.VERIFY
                if category == RiskCategory.MEDIUM
                else RecommendedAction.HOLD
            )
        ),
    )
    anomaly = AnomalyEvaluation(
        contribution=12,
        raw_score=-0.42,
        deviations=("amount_ratio",),
        model_version="model-v1",
        inference_time_ms=1.2,
    )
    combined = CombinedRiskScore(
        rules_contribution=20,
        anomaly_contribution=12,
        uncapped_score=32,
        final_score=32,
        policy_version="score-v1",
        override_action=rules.override_action,
    )
    action = {
        RiskCategory.LOW: RecommendedAction.APPROVE,
        RiskCategory.MEDIUM: RecommendedAction.VERIFY,
        RiskCategory.HIGH: RecommendedAction.HOLD,
    }[category]
    decision = RiskDecision(
        final_score=32,
        score_category=RiskCategory.LOW,
        category=category,
        action=action,
        explanation=("Observed elevated amount.",),
        decision_policy_version="decision-v1",
        processing_latency_ms=3.4,
        override_applied=category != RiskCategory.LOW,
    )
    return DecisionOutputFactory().build(
        event,
        rules,
        anomaly,
        combined,
        decision,
        processed_at=datetime(2026, 7, 24, tzinfo=timezone.utc),
    )


class DecisionOutputFactoryTests(unittest.TestCase):
    def test_every_decision_produces_one_valid_scored_event(self) -> None:
        outputs = make_outputs()

        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0].topic, "transactions.scored")
        parsed = parse_decision_event(outputs[0].payload)
        self.assertIsInstance(parsed, ScoredTransactionEvent)
        self.assertEqual(parsed.risk.final_score, 32)
        self.assertEqual(parsed.risk.category, "low")
        self.assertEqual(parsed.schema_version, "1.0")
        json.dumps(outputs[0].payload)

    def test_actionable_decision_also_produces_a_valid_alert(self) -> None:
        for category in (RiskCategory.MEDIUM, RiskCategory.HIGH):
            with self.subTest(category=category):
                outputs = make_outputs(category=category)

                self.assertEqual(
                    [item.topic for item in outputs],
                    ["transactions.scored", "fraud.alerts"],
                )
                alert = parse_decision_event(outputs[1].payload)
                self.assertIsInstance(alert, FraudAlertEvent)
                self.assertEqual(alert.risk_category, category.value)
                self.assertIsNotNone(alert.score_override_action)
                self.assertEqual(
                    alert.score_event_id,
                    outputs[0].payload["event_id"],
                )
                self.assertEqual(
                    alert.correlation_id,
                    outputs[0].payload["correlation_id"],
                )

    def test_output_identifiers_and_partition_key_are_deterministic(self) -> None:
        first = make_outputs(category=RiskCategory.HIGH)
        second = make_outputs(category=RiskCategory.HIGH)

        self.assertEqual(
            [item.outbox_id for item in first],
            [item.outbox_id for item in second],
        )
        self.assertEqual(
            [item.key for item in first],
            [item.key for item in second],
        )


class DecisionEventContractTests(unittest.TestCase):
    def test_unknown_fields_are_rejected(self) -> None:
        payload = copy.deepcopy(make_outputs()[0].payload)
        payload["unexpected"] = "not part of schema 1.0"

        with self.assertRaises(ValidationError):
            parse_decision_event(payload)

    def test_naive_event_time_is_rejected(self) -> None:
        payload = copy.deepcopy(make_outputs()[0].payload)
        payload["event_time"] = "2026-07-24T10:00:00"

        with self.assertRaisesRegex(ValidationError, "timezone"):
            parse_decision_event(payload)

    def test_inconsistent_score_category_is_rejected(self) -> None:
        payload = copy.deepcopy(make_outputs()[0].payload)
        payload["risk"]["score_category"] = "high"

        with self.assertRaisesRegex(ValidationError, "score_category"):
            parse_decision_event(payload)

    def test_low_risk_payload_cannot_be_disguised_as_an_alert(self) -> None:
        alert = copy.deepcopy(
            make_outputs(category=RiskCategory.HIGH)[1].payload
        )
        alert["risk_category"] = "low"
        alert["recommended_action"] = "approve"

        with self.assertRaises(ValidationError):
            parse_decision_event(alert)


class KafkaOutputPublisherTests(unittest.TestCase):
    def test_publishes_contract_events_to_their_outbox_topics(self) -> None:
        client = FakeProducerClient()
        publisher = KafkaOutputPublisher(client)
        outputs = make_outputs(category=RiskCategory.HIGH)

        receipts = [publisher.publish(message) for message in outputs]

        self.assertEqual(
            [record["topic"] for record in client.records],
            ["transactions.scored", "fraud.alerts"],
        )
        self.assertEqual(
            [receipt.topic for receipt in receipts],
            ["transactions.scored", "fraud.alerts"],
        )
        for record, output in zip(client.records, outputs):
            self.assertIsInstance(record["headers"], list)
            self.assertEqual(
                record["key"],
                output.key.encode("utf-8"),
            )
            self.assertEqual(
                json.loads(record["value"]),
                output.payload,
            )
            headers = dict(record["headers"])
            self.assertEqual(
                headers["outbox-id"],
                output.outbox_id.encode("utf-8"),
            )

    def test_rejects_topic_and_contract_mismatches_before_kafka(self) -> None:
        client = FakeProducerClient()
        publisher = KafkaOutputPublisher(client)
        scored = make_outputs()[0]
        wrong_topic = OutboxMessage(
            outbox_id=scored.outbox_id,
            record_id=scored.record_id,
            topic="fraud.alerts",
            key=scored.key,
            payload=scored.payload,
        )

        with self.assertRaisesRegex(ValueError, "requires an alert"):
            publisher.publish(wrong_topic)
        self.assertEqual(client.records, [])


if __name__ == "__main__":
    unittest.main()
