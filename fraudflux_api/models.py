"""HTTP request and response contracts for the FraudFlux API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Mapping, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from fraudflux_storage import ReviewOutcome


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())


class ScoreResponse(ApiModel):
    event_id: str
    transaction_id: str
    customer_id: str
    created: bool
    final_score: int = Field(ge=0, le=100)
    score_category: Literal["low", "medium", "high"]
    category: Literal["low", "medium", "high"]
    recommended_action: Literal[
        "approve",
        "additional_verification",
        "hold_for_review",
    ]
    override_applied: bool
    explanation: tuple[str, ...]
    rules_contribution: int
    triggered_rules: tuple[Mapping[str, Any], ...]
    anomaly_contribution: int
    anomaly_level: str
    anomaly_deviations: tuple[str, ...]
    ruleset_version: str
    model_version: str
    score_policy_version: str
    decision_policy_version: str
    processing_latency_ms: float
    processed_at: datetime


class TransactionSummary(ApiModel):
    transaction_id: str
    customer_id: str
    amount_minor: int
    currency: str
    merchant_id: str
    transaction_time: datetime
    processing_status: str
    final_score: int
    category: Literal["low", "medium", "high"]
    recommended_action: str
    processed_at: datetime


class TransactionDetail(ApiModel):
    transaction_id: str
    event_id: str
    customer_id: str
    account_id: str
    amount_minor: int
    currency: str
    merchant_id: str
    merchant_category: str
    device_id: str
    region: str
    country: str
    transaction_time: datetime
    processing_status: str
    final_score: int
    score_category: str
    category: str
    recommended_action: str
    override_applied: bool
    explanation: tuple[str, ...]
    rules_contribution: int
    rule_hits: tuple[Mapping[str, Any], ...]
    anomaly_contribution: int
    anomaly_level: str
    anomaly_deviations: tuple[str, ...]
    ruleset_version: str
    model_version: str
    score_policy_version: str
    decision_policy_version: str
    processing_latency_ms: float
    processed_at: datetime


class AlertSummary(ApiModel):
    alert_id: str
    transaction_id: str
    customer_id: str
    status: Literal["open", "assigned", "resolved"]
    assigned_to: Optional[str]
    created_at: datetime
    updated_at: datetime
    final_score: int
    category: Literal["medium", "high"]
    recommended_action: str


class AlertDetail(AlertSummary):
    assigned_at: Optional[datetime]
    score_category: str
    override_applied: bool
    explanation: tuple[str, ...]
    rules_contribution: int
    rule_hits: tuple[Mapping[str, Any], ...]
    anomaly_contribution: int
    anomaly_level: str
    anomaly_deviations: tuple[str, ...]
    ruleset_version: str
    model_version: str
    score_policy_version: str
    decision_policy_version: str
    processing_latency_ms: float
    review_id: Optional[str] = None
    analyst_id: Optional[str] = None
    review_outcome: Optional[ReviewOutcome] = None
    review_notes: Optional[str] = None
    reviewed_at: Optional[datetime] = None


class AnalystReviewRequest(ApiModel):
    review_id: str = Field(
        default_factory=lambda: f"REVIEW-{uuid4()}",
        min_length=1,
        max_length=140,
    )
    analyst_id: str = Field(min_length=1, max_length=120)
    outcome: ReviewOutcome
    notes: Optional[str] = Field(default=None, max_length=5000)


class AnalystReviewResponse(ApiModel):
    alert_id: str
    review_id: str
    outcome: ReviewOutcome
    status: Literal["resolved"]


class DashboardSummary(ApiModel):
    total_transactions: int = 0
    low_risk: int = 0
    medium_risk: int = 0
    high_risk: int = 0
    average_risk_score: float = 0
    p95_processing_latency_ms: float = 0
    open_alerts: int = 0
    assigned_alerts: int = 0
    resolved_alerts: int = 0


class HealthResponse(ApiModel):
    status: Literal["healthy", "degraded"]
    service: str
    version: str
    checks: Mapping[str, Literal["healthy", "unhealthy"]]
