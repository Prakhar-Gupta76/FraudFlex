"""Evaluation input and report contracts."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, model_validator


RiskCategoryValue = Literal["low", "medium", "high"]
ActionValue = Literal[
    "approve",
    "additional_verification",
    "hold_for_review",
]


class LabelSource(str, Enum):
    SIMULATOR = "simulator_ground_truth"
    ANALYST = "validated_analyst_label"


class EvaluationModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        protected_namespaces=(),
    )


class EvaluationCase(EvaluationModel):
    transaction_id: str = Field(min_length=1, max_length=64)
    amount_minor: int = Field(gt=0)
    is_fraud: bool
    label_source: LabelSource
    final_score: int = Field(ge=0, le=100)
    category: RiskCategoryValue
    recommended_action: ActionValue
    label_context: str | None = Field(default=None, max_length=200)


class EvaluationPolicy(EvaluationModel):
    version: str = Field(default="evaluation-policy-1.0.0", min_length=1)
    positive_categories: tuple[RiskCategoryValue, ...] = ("medium", "high")

    @model_validator(mode="after")
    def validate_categories(self) -> "EvaluationPolicy":
        if not self.positive_categories:
            raise ValueError("positive_categories cannot be empty")
        if len(set(self.positive_categories)) != len(
            self.positive_categories
        ):
            raise ValueError("positive_categories must be unique")
        return self


class EvaluationReport(EvaluationModel):
    policy_version: str
    generated_at: datetime
    evaluated_transactions: int
    positive_labels: int
    negative_labels: int
    label_source_counts: Mapping[str, int]
    true_positives: int
    true_negatives: int
    false_positives: int
    false_negatives: int
    precision: float
    recall: float
    f1_score: float
    precision_recall_auc: float
    false_positive_rate: float
    fraud_amount_detected_minor: int
    legitimate_amount_incorrectly_held_minor: int
