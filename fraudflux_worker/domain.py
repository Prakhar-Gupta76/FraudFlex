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

    def __post_init__(self) -> None:
        if not 0 <= self.final_score <= 100:
            raise ValueError("final score must be between 0 and 100")
        if not self.explanation:
            raise ValueError("decision explanation cannot be empty")


@dataclass(frozen=True)
class StoredDecision:
    record_id: str
    input_event_id: str
    transaction_id: str
    customer_id: str
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
