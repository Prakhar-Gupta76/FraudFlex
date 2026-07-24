"""Initial score-to-action policy and factual explanation generation."""

from __future__ import annotations

import math
import time
from typing import Callable

from fraudflux_worker import (
    AnomalyEvaluation,
    CombinedRiskScore,
    RecommendedAction,
    RiskCategory,
    RiskDecision,
    RuleEvaluation,
)


class InitialDecisionEngine:
    POLICY_VERSION = "decision-policy-1.0.0"

    def __init__(
        self,
        *,
        timer_ns: Callable[[], int] = time.perf_counter_ns,
    ) -> None:
        self.timer_ns = timer_ns

    def decide(
        self,
        score: CombinedRiskScore,
        rules: RuleEvaluation,
        anomaly: AnomalyEvaluation,
        *,
        upstream_processing_latency_ms: float = 0.0,
    ) -> RiskDecision:
        if (
            not math.isfinite(upstream_processing_latency_ms)
            or upstream_processing_latency_ms < 0
        ):
            raise ValueError(
                "upstream processing latency must be finite and non-negative"
            )
        started = self.timer_ns()
        score_category = _score_category(score.final_score)
        category = _effective_category(
            score_category,
            score.override_action,
        )
        action = _action(category)
        override_applied = category != score_category
        explanation = _explain(
            score,
            rules,
            anomaly,
            score_category,
            category,
            action,
            override_applied,
        )
        decision_latency_ms = max(
            0.0,
            (self.timer_ns() - started) / 1_000_000,
        )
        return RiskDecision(
            final_score=score.final_score,
            score_category=score_category,
            category=category,
            action=action,
            explanation=explanation,
            decision_policy_version=self.POLICY_VERSION,
            processing_latency_ms=round(
                upstream_processing_latency_ms + decision_latency_ms,
                3,
            ),
            override_applied=override_applied,
        )


def _score_category(score: int) -> RiskCategory:
    if score <= 39:
        return RiskCategory.LOW
    if score <= 69:
        return RiskCategory.MEDIUM
    return RiskCategory.HIGH


def _effective_category(
    score_category: RiskCategory,
    override: RecommendedAction | None,
) -> RiskCategory:
    override_category = {
        None: RiskCategory.LOW,
        RecommendedAction.VERIFY: RiskCategory.MEDIUM,
        RecommendedAction.HOLD: RiskCategory.HIGH,
    }.get(override)
    if override_category is None:
        raise ValueError("decision override cannot force approval")
    severity = {
        RiskCategory.LOW: 0,
        RiskCategory.MEDIUM: 1,
        RiskCategory.HIGH: 2,
    }
    return max(
        (score_category, override_category),
        key=severity.__getitem__,
    )


def _action(category: RiskCategory) -> RecommendedAction:
    return {
        RiskCategory.LOW: RecommendedAction.APPROVE,
        RiskCategory.MEDIUM: RecommendedAction.VERIFY,
        RiskCategory.HIGH: RecommendedAction.HOLD,
    }[category]


def _explain(
    score: CombinedRiskScore,
    rules: RuleEvaluation,
    anomaly: AnomalyEvaluation,
    score_category: RiskCategory,
    category: RiskCategory,
    action: RecommendedAction,
    override_applied: bool,
) -> tuple[str, ...]:
    statements = [
        (
            f"Final score {score.final_score}/100 consists of "
            f"{score.rules_contribution} rules points and "
            f"{score.anomaly_contribution} anomaly points."
        ),
        (
            f"The numeric score falls in the {score_category.value} "
            f"category under {InitialDecisionEngine.POLICY_VERSION}."
        ),
    ]
    for hit in rules.hits:
        reason = hit.reason.rstrip().rstrip(".")
        statements.append(
            f"Observed rule condition {hit.rule_id} contributed "
            f"{hit.points} points: {reason}."
        )
    statements.append(
        f"The anomaly model contributed {anomaly.contribution} points "
        f"and rated the pattern {anomaly.level.replace('_', ' ')}."
    )
    statements.extend(
        f"Model-observed deviation: {deviation.rstrip().rstrip('.')}."
        for deviation in anomaly.deviations
    )
    if score.override_action is not None:
        effect = (
            f"raised the effective category to {category.value}"
            if override_applied
            else "was already satisfied by the numeric score"
        )
        statements.append(
            f"The configured {score.override_action.value} override "
            f"{effect}."
        )
    statements.append(
        f"The recommended MVP action is {action.value}."
    )
    return tuple(statements)
