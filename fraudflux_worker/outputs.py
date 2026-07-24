"""Deterministic scored-event and alert construction."""

from __future__ import annotations

from datetime import datetime
from typing import List

from fraudflux_events import (
    FraudAlertEvent,
    RiskDecisionEvent,
    ScoredTransactionEvent,
    TriggeredRuleEvent,
)
from fraudflux_validation import TransactionEvent

from .domain import (
    AnomalyEvaluation,
    CombinedRiskScore,
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
        combined_score: CombinedRiskScore,
        decision: RiskDecision,
        *,
        processed_at: datetime,
    ) -> List[OutboxMessage]:
        record_id = f"event:{event.event_id}"
        score_event_id = f"SCORED-{event.event_id}"
        triggered_rules = tuple(
            TriggeredRuleEvent(
                rule_id=hit.rule_id,
                points=hit.points,
                reason=hit.reason,
            )
            for hit in rules.hits
        )
        risk = RiskDecisionEvent(
            rules_contribution=combined_score.rules_contribution,
            anomaly_contribution=combined_score.anomaly_contribution,
            uncapped_score=combined_score.uncapped_score,
            score_policy_version=combined_score.policy_version,
            score_override_action=(
                combined_score.override_action.value
                if combined_score.override_action
                else None
            ),
            anomaly_level=anomaly.level,
            anomaly_inference_time_ms=anomaly.inference_time_ms,
            final_score=combined_score.final_score,
            score_category=decision.score_category.value,
            category=decision.category.value,
            recommended_action=decision.action.value,
            override_applied=decision.override_applied,
            explanation=decision.explanation,
            triggered_rules=triggered_rules,
            anomaly_deviations=anomaly.deviations,
            ruleset_version=rules.ruleset_version,
            model_version=anomaly.model_version,
            decision_policy_version=decision.decision_policy_version,
            processing_latency_ms=decision.processing_latency_ms,
        )
        score_event = ScoredTransactionEvent(
            event_id=score_event_id,
            event_time=processed_at,
            correlation_id=event.event_id,
            transaction_id=event.transaction.transaction_id,
            customer_id=event.transaction.customer_id,
            risk=risk,
        )
        outputs = [
            OutboxMessage(
                outbox_id=f"OUTBOX-{score_event_id}",
                record_id=record_id,
                topic=self.scored_topic,
                key=event.transaction.customer_id,
                payload=score_event.model_dump(mode="json"),
            )
        ]

        if decision.category in {RiskCategory.MEDIUM, RiskCategory.HIGH}:
            alert_event_id = f"ALERT-{event.event_id}"
            alert_event = FraudAlertEvent(
                event_id=alert_event_id,
                event_time=processed_at,
                correlation_id=event.event_id,
                score_event_id=score_event_id,
                transaction_id=event.transaction.transaction_id,
                customer_id=event.transaction.customer_id,
                risk_score=combined_score.final_score,
                score_category=decision.score_category.value,
                risk_category=decision.category.value,
                recommended_action=decision.action.value,
                score_override_action=(
                    combined_score.override_action.value
                    if combined_score.override_action
                    else None
                ),
                override_applied=decision.override_applied,
                explanation=decision.explanation,
                rules_contribution=combined_score.rules_contribution,
                anomaly_contribution=(
                    combined_score.anomaly_contribution
                ),
                triggered_rules=triggered_rules,
                anomaly_deviations=anomaly.deviations,
                ruleset_version=rules.ruleset_version,
                model_version=anomaly.model_version,
                score_policy_version=combined_score.policy_version,
                decision_policy_version=(
                    decision.decision_policy_version
                ),
                processing_latency_ms=decision.processing_latency_ms,
            )
            outputs.append(
                OutboxMessage(
                    outbox_id=f"OUTBOX-{alert_event_id}",
                    record_id=record_id,
                    topic=self.alert_topic,
                    key=event.transaction.customer_id,
                    payload=alert_event.model_dump(mode="json"),
                )
            )
        return outputs
