"""Worker domain values independent of infrastructure implementations."""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Optional, Tuple


class RiskCategory(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class RecommendedAction(str, Enum):
    APPROVE = "approve"
    VERIFY = "additional_verification"
    HOLD = "hold_for_review"


class ProcessingOutcome(str, Enum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"
    NO_MESSAGE = "no_message"


@dataclass(frozen=True)
class CustomerHistory:
    customer_id: str
    values: Mapping[str, Any]


@dataclass(frozen=True)
class FeatureSet:
    values: Mapping[str, Any]


@dataclass(frozen=True)
class RuleHit:
    rule_id: str
    points: int
    reason: str

    def __post_init__(self) -> None:
        if self.points < 0:
            raise ValueError("rule points cannot be negative")


@dataclass(frozen=True)
class RuleEvaluation:
    contribution: int
    hits: Tuple[RuleHit, ...]
    ruleset_version: str
    override_action: Optional[RecommendedAction] = None

    def __post_init__(self) -> None:
        if not 0 <= self.contribution <= 70:
            raise ValueError("rules contribution must be between 0 and 70")


@dataclass(frozen=True)
class AnomalyEvaluation:
    contribution: int
    raw_score: float
    deviations: Tuple[str, ...]
    model_version: str
    inference_time_ms: float = 0.0

    def __post_init__(self) -> None:
        if not 0 <= self.contribution <= 30:
            raise ValueError("anomaly contribution must be between 0 and 30")
        if not math.isfinite(self.raw_score):
            raise ValueError("anomaly raw score must be finite")
        if (
            not math.isfinite(self.inference_time_ms)
            or self.inference_time_ms < 0
        ):
            raise ValueError(
                "anomaly inference time must be finite and non-negative"
            )
        if not self.model_version.strip():
            raise ValueError("anomaly model version cannot be blank")

    @property
    def level(self) -> str:
        if self.contribution <= 5:
            return "normal"
        if self.contribution <= 10:
            return "slightly_unusual"
        if self.contribution <= 20:
            return "moderately_unusual"
        return "highly_unusual"


@dataclass(frozen=True)
class CombinedRiskScore:
    rules_contribution: int
    anomaly_contribution: int
    uncapped_score: int
    final_score: int
    policy_version: str
    override_action: Optional[RecommendedAction] = None

    def __post_init__(self) -> None:
        integer_values = (
            self.rules_contribution,
            self.anomaly_contribution,
            self.uncapped_score,
            self.final_score,
        )
        if any(type(value) is not int for value in integer_values):
            raise ValueError("combined risk score values must be integers")
        if not 0 <= self.rules_contribution <= 70:
            raise ValueError("combined rules contribution must be 0 to 70")
        if not 0 <= self.anomaly_contribution <= 30:
            raise ValueError("combined anomaly contribution must be 0 to 30")
        expected_uncapped = (
            self.rules_contribution + self.anomaly_contribution
        )
        if self.uncapped_score != expected_uncapped:
            raise ValueError(
                "uncapped score must equal rules plus anomaly contributions"
            )
        if self.final_score != min(100, self.uncapped_score):
            raise ValueError("final score must apply the 100-point cap")
        if not self.policy_version.strip():
            raise ValueError("score policy version cannot be blank")
        if self.override_action == RecommendedAction.APPROVE:
            raise ValueError("a risk override cannot force approval")

    @property
    def requires_review(self) -> bool:
        return self.override_action in {
            RecommendedAction.VERIFY,
            RecommendedAction.HOLD,
        }


@dataclass(frozen=True)
class RiskDecision:
    final_score: int
    category: RiskCategory
    action: RecommendedAction
    explanation: Tuple[str, ...]
    score_category: Optional[RiskCategory] = None
    decision_policy_version: str = "legacy"
    processing_latency_ms: float = 0.0
    override_applied: bool = False

    def __post_init__(self) -> None:
        if type(self.final_score) is not int:
            raise ValueError("final score must be an integer")
        if not 0 <= self.final_score <= 100:
            raise ValueError("final score must be between 0 and 100")
        if not self.explanation:
            raise ValueError("decision explanation cannot be empty")
        expected_score_category = (
            RiskCategory.LOW
            if self.final_score <= 39
            else (
                RiskCategory.MEDIUM
                if self.final_score <= 69
                else RiskCategory.HIGH
            )
        )
        if self.score_category is None:
            object.__setattr__(
                self,
                "score_category",
                expected_score_category,
            )
        elif self.score_category != expected_score_category:
            raise ValueError(
                "score category does not match the final score"
            )
        severity = {
            RiskCategory.LOW: 0,
            RiskCategory.MEDIUM: 1,
            RiskCategory.HIGH: 2,
        }
        if severity[self.category] < severity[expected_score_category]:
            raise ValueError("effective category cannot reduce score category")
        if (
            self.category != expected_score_category
            and not self.override_applied
        ):
            raise ValueError(
                "effective category can change only through an override"
            )
        expected_action = {
            RiskCategory.LOW: RecommendedAction.APPROVE,
            RiskCategory.MEDIUM: RecommendedAction.VERIFY,
            RiskCategory.HIGH: RecommendedAction.HOLD,
        }[self.category]
        if self.action != expected_action:
            raise ValueError("recommended action does not match category")
        if not self.decision_policy_version.strip():
            raise ValueError("decision policy version cannot be blank")
        if (
            not math.isfinite(self.processing_latency_ms)
            or self.processing_latency_ms < 0
        ):
            raise ValueError(
                "processing latency must be finite and non-negative"
            )


@dataclass(frozen=True)
class StoredDecision:
    record_id: str
    input_event_id: str
    transaction_id: str
    customer_id: str
    transaction_payload: Mapping[str, Any]
    feature_values: Mapping[str, Any]
    rules: RuleEvaluation
    anomaly: AnomalyEvaluation
    combined_score: CombinedRiskScore
    decision: RiskDecision
    processed_at: str


@dataclass(frozen=True)
class OutboxMessage:
    outbox_id: str
    record_id: str
    topic: str
    key: str
    payload: Mapping[str, Any]
