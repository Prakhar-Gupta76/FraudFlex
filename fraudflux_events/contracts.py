"""Versioned contracts for FraudFlux scored and alert events."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal, Mapping, Optional, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
    model_validator,
)


RiskCategoryValue = Literal["low", "medium", "high"]
RecommendedActionValue = Literal[
    "approve",
    "additional_verification",
    "hold_for_review",
]
OverrideActionValue = Literal[
    "additional_verification",
    "hold_for_review",
]


class StrictEventModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        protected_namespaces=(),
    )


class TriggeredRuleEvent(StrictEventModel):
    rule_id: str = Field(min_length=1, max_length=120)
    points: int = Field(ge=0, le=70)
    reason: str = Field(min_length=1, max_length=1000)


class RiskDecisionEvent(StrictEventModel):
    rules_contribution: int = Field(ge=0, le=70)
    anomaly_contribution: int = Field(ge=0, le=30)
    uncapped_score: int = Field(ge=0, le=100)
    score_policy_version: str = Field(min_length=1, max_length=80)
    score_override_action: Optional[OverrideActionValue] = None
    anomaly_level: Literal[
        "normal",
        "slightly_unusual",
        "moderately_unusual",
        "highly_unusual",
    ]
    anomaly_inference_time_ms: float = Field(ge=0, allow_inf_nan=False)
    final_score: int = Field(ge=0, le=100)
    score_category: RiskCategoryValue
    category: RiskCategoryValue
    recommended_action: RecommendedActionValue
    override_applied: bool
    explanation: tuple[str, ...] = Field(min_length=1)
    triggered_rules: tuple[TriggeredRuleEvent, ...]
    anomaly_deviations: tuple[str, ...]
    ruleset_version: str = Field(min_length=1, max_length=80)
    model_version: str = Field(min_length=1, max_length=80)
    decision_policy_version: str = Field(min_length=1, max_length=80)
    processing_latency_ms: float = Field(ge=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def validate_decision_consistency(self) -> "RiskDecisionEvent":
        expected_score = self.rules_contribution + self.anomaly_contribution
        if self.uncapped_score != expected_score:
            raise ValueError(
                "uncapped_score must equal rules plus anomaly contributions"
            )
        if self.final_score != min(100, self.uncapped_score):
            raise ValueError("final_score must apply the 100-point cap")
        expected_category = _category_for_score(self.final_score)
        if self.score_category != expected_category:
            raise ValueError("score_category does not match final_score")
        if _severity(self.category) < _severity(self.score_category):
            raise ValueError("category cannot reduce score_category")
        expected_action = _action_for_category(self.category)
        if self.recommended_action != expected_action:
            raise ValueError("recommended_action does not match category")
        category_changed = self.category != self.score_category
        if self.override_applied != category_changed:
            raise ValueError("override_applied is inconsistent with category")
        if category_changed and self.score_override_action is None:
            raise ValueError(
                "an elevated category requires score_override_action"
            )
        if self.score_override_action is not None:
            required = {
                "additional_verification": 1,
                "hold_for_review": 2,
            }[self.score_override_action]
            if _severity(self.category) < required:
                raise ValueError("category does not honor score override")
        return self


class ScoredTransactionEvent(StrictEventModel):
    event_id: str = Field(min_length=1, max_length=140)
    event_type: Literal["transaction.scored"] = "transaction.scored"
    schema_version: Literal["1.0"] = "1.0"
    event_time: datetime
    correlation_id: str = Field(min_length=1, max_length=64)
    transaction_id: str = Field(min_length=1, max_length=64)
    customer_id: str = Field(min_length=1, max_length=64)
    risk: RiskDecisionEvent

    _event_time_is_aware = field_validator("event_time")(
        lambda value: _aware_datetime(value)
    )


class FraudAlertEvent(StrictEventModel):
    event_id: str = Field(min_length=1, max_length=140)
    event_type: Literal["fraud.alert.created"] = "fraud.alert.created"
    schema_version: Literal["1.0"] = "1.0"
    event_time: datetime
    correlation_id: str = Field(min_length=1, max_length=64)
    score_event_id: str = Field(min_length=1, max_length=140)
    transaction_id: str = Field(min_length=1, max_length=64)
    customer_id: str = Field(min_length=1, max_length=64)
    risk_score: int = Field(ge=0, le=100)
    score_category: RiskCategoryValue
    risk_category: Literal["medium", "high"]
    recommended_action: Literal[
        "additional_verification",
        "hold_for_review",
    ]
    score_override_action: Optional[OverrideActionValue] = None
    override_applied: bool
    explanation: tuple[str, ...] = Field(min_length=1)
    rules_contribution: int = Field(ge=0, le=70)
    anomaly_contribution: int = Field(ge=0, le=30)
    triggered_rules: tuple[TriggeredRuleEvent, ...]
    anomaly_deviations: tuple[str, ...]
    ruleset_version: str = Field(min_length=1, max_length=80)
    model_version: str = Field(min_length=1, max_length=80)
    score_policy_version: str = Field(min_length=1, max_length=80)
    decision_policy_version: str = Field(min_length=1, max_length=80)
    processing_latency_ms: float = Field(ge=0, allow_inf_nan=False)

    _event_time_is_aware = field_validator("event_time")(
        lambda value: _aware_datetime(value)
    )

    @model_validator(mode="after")
    def validate_alert_consistency(self) -> "FraudAlertEvent":
        if self.risk_score != (
            self.rules_contribution + self.anomaly_contribution
        ):
            raise ValueError("risk_score must equal its score contributions")
        if self.score_category != _category_for_score(self.risk_score):
            raise ValueError("score_category does not match risk_score")
        if _severity(self.risk_category) < _severity(self.score_category):
            raise ValueError("risk_category cannot reduce score_category")
        if self.recommended_action != _action_for_category(
            self.risk_category
        ):
            raise ValueError(
                "recommended_action does not match risk_category"
            )
        changed = self.risk_category != self.score_category
        if self.override_applied != changed:
            raise ValueError(
                "override_applied is inconsistent with risk_category"
            )
        if changed and self.score_override_action is None:
            raise ValueError(
                "an elevated risk_category requires score_override_action"
            )
        if self.score_override_action is not None:
            required = {
                "additional_verification": 1,
                "hold_for_review": 2,
            }[self.score_override_action]
            if _severity(self.risk_category) < required:
                raise ValueError("risk_category does not honor score override")
        return self


DecisionEvent = Annotated[
    Union[ScoredTransactionEvent, FraudAlertEvent],
    Field(discriminator="event_type"),
]
_DECISION_EVENT_ADAPTER = TypeAdapter(DecisionEvent)


def parse_decision_event(payload: Mapping[str, Any]) -> DecisionEvent:
    """Validate a scored or alert payload for a downstream consumer."""
    return _DECISION_EVENT_ADAPTER.validate_python(payload)


def _aware_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("event_time must include a timezone")
    return value


def _category_for_score(score: int) -> RiskCategoryValue:
    if score <= 39:
        return "low"
    if score <= 69:
        return "medium"
    return "high"


def _severity(category: RiskCategoryValue) -> int:
    return {"low": 0, "medium": 1, "high": 2}[category]


def _action_for_category(
    category: RiskCategoryValue,
) -> RecommendedActionValue:
    return {
        "low": "approve",
        "medium": "additional_verification",
        "high": "hold_for_review",
    }[category]
