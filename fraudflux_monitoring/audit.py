"""Immutable, self-contained audit view of a stored risk decision."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from fraudflux_validation import validate_transaction_event

if TYPE_CHECKING:
    from fraudflux_worker.domain import StoredDecision


@dataclass(frozen=True)
class DecisionAuditSnapshot:
    event_id: str
    transaction_id: str
    event_time: datetime
    processed_at: datetime
    ruleset_version: str
    model_version: str
    triggered_reasons: tuple[str, ...]
    final_score: int
    category: str
    recommended_action: str

    @classmethod
    def from_stored(
        cls,
        decision: "StoredDecision",
    ) -> "DecisionAuditSnapshot":
        event = validate_transaction_event(decision.transaction_payload)
        processed_at = datetime.fromisoformat(decision.processed_at)
        if processed_at.tzinfo is None:
            raise ValueError("stored processing time must be timezone-aware")
        if event.event_id != decision.input_event_id:
            raise ValueError("stored audit event ID is inconsistent")
        return cls(
            event_id=event.event_id,
            transaction_id=decision.transaction_id,
            event_time=event.event_time,
            processed_at=processed_at,
            ruleset_version=decision.rules.ruleset_version,
            model_version=decision.anomaly.model_version,
            triggered_reasons=tuple(
                hit.reason for hit in decision.rules.hits
            ),
            final_score=decision.combined_score.final_score,
            category=decision.decision.category.value,
            recommended_action=decision.decision.action.value,
        )
