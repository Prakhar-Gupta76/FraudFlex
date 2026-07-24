"""Deterministic scored-event and alert construction."""

from __future__ import annotations

from datetime import datetime
from typing import List

from fraudflux_validation import TransactionEvent

from .domain import (
    AnomalyEvaluation,
    OutboxMessage,
    RiskCategory,
    RiskDecision,
    RuleEvaluation,
)


class DecisionOutputFactory:
    scored_topic = "transactions.scored"
    alert_topic = "fraud.alerts"
    dead_letter_topic = "transactions.dead-letter"

    def build(
        self,
        event: TransactionEvent,
        rules: RuleEvaluation,
        anomaly: AnomalyEvaluation,
        decision: RiskDecision,
        *,
        processed_at: datetime,
    ) -> List[OutboxMessage]:
        record_id = f"event:{event.event_id}"
        score_event_id = f"SCORED-{event.event_id}"
        score_payload = {
            "event_id": score_event_id,
            "event_type": "transaction.scored",
            "schema_version": "1.0",
            "event_time": processed_at.isoformat(),
            "correlation_id": event.event_id,
            "transaction_id": event.transaction.transaction_id,
            "customer_id": event.transaction.customer_id,
            "risk": {
                "rules_contribution": rules.contribution,
                "anomaly_contribution": anomaly.contribution,
                "anomaly_level": anomaly.level,
                "anomaly_inference_time_ms": anomaly.inference_time_ms,
                "final_score": decision.final_score,
                "category": decision.category.value,
                "recommended_action": decision.action.value,
                "explanation": list(decision.explanation),
                "triggered_rules": [
                    {
                        "rule_id": hit.rule_id,
                        "points": hit.points,
                        "reason": hit.reason,
                    }
                    for hit in rules.hits
                ],
                "anomaly_deviations": list(anomaly.deviations),
                "ruleset_version": rules.ruleset_version,
                "model_version": anomaly.model_version,
            },
        }
        outputs = [
            OutboxMessage(
                outbox_id=f"OUTBOX-{score_event_id}",
                record_id=record_id,
                topic=self.scored_topic,
                key=event.transaction.customer_id,
                payload=score_payload,
            )
        ]

        if decision.category in {RiskCategory.MEDIUM, RiskCategory.HIGH}:
            alert_event_id = f"ALERT-{event.event_id}"
            outputs.append(
                OutboxMessage(
                    outbox_id=f"OUTBOX-{alert_event_id}",
                    record_id=record_id,
                    topic=self.alert_topic,
                    key=event.transaction.customer_id,
                    payload={
                        "event_id": alert_event_id,
                        "event_type": "fraud.alert.created",
                        "schema_version": "1.0",
                        "event_time": processed_at.isoformat(),
                        "correlation_id": event.event_id,
                        "score_event_id": score_event_id,
                        "transaction_id": event.transaction.transaction_id,
                        "customer_id": event.transaction.customer_id,
                        "risk_score": decision.final_score,
                        "risk_category": decision.category.value,
                        "recommended_action": decision.action.value,
                        "explanation": list(decision.explanation),
                    },
                )
            )
        return outputs
