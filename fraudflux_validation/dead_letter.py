"""Dead-letter event creation for invalid transaction events."""

from __future__ import annotations

import json
from base64 import b64encode
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Mapping, Optional, Tuple
from uuid import uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from .service import TransactionValidationError


class DeadLetterSource(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    topic: str = Field(default="transactions.raw", min_length=1, max_length=249)
    partition: Optional[int] = Field(default=None, ge=0)
    offset: Optional[int] = Field(default=None, ge=0)
    consumer_group: Optional[str] = None


class DeadLetterEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    dead_letter_id: str
    event_type: Literal["transaction.validation_failed"]
    schema_version: Literal["1.0"]
    failed_at: AwareDatetime
    reason: Literal["transaction_contract_invalid"]
    original_event_id: Optional[str]
    source: DeadLetterSource
    errors: List[Dict[str, str]]
    original_payload: Any
    original_payload_encoding: Literal["structured", "text", "base64"]


def build_dead_letter_event(
    original_payload: Any,
    error: TransactionValidationError,
    *,
    source: Optional[DeadLetterSource] = None,
    failed_at: Optional[datetime] = None,
) -> DeadLetterEvent:
    """Create the record a future consumer will publish to the DLQ topic."""
    normalized_payload, payload_encoding = _normalize_payload(original_payload)
    return DeadLetterEvent(
        dead_letter_id=f"DLQ-{uuid4()}",
        event_type="transaction.validation_failed",
        schema_version="1.0",
        failed_at=failed_at or datetime.now(timezone.utc),
        reason="transaction_contract_invalid",
        original_event_id=_extract_event_id(original_payload),
        source=source or DeadLetterSource(),
        errors=[issue.as_dict() for issue in error.issues],
        original_payload=normalized_payload,
        original_payload_encoding=payload_encoding,
    )


def _extract_event_id(payload: Any) -> Optional[str]:
    candidate: Any = payload
    if isinstance(payload, (str, bytes, bytearray)):
        try:
            candidate = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
            return None

    if not isinstance(candidate, Mapping):
        return None
    event_id = candidate.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip():
        return None
    return event_id.strip()


def _normalize_payload(
    payload: Any,
) -> Tuple[Any, Literal["structured", "text", "base64"]]:
    if isinstance(payload, bytearray):
        payload = bytes(payload)
    if isinstance(payload, bytes):
        try:
            return payload.decode("utf-8"), "text"
        except UnicodeDecodeError:
            return b64encode(payload).decode("ascii"), "base64"
    if isinstance(payload, str):
        return payload, "text"
    return payload, "structured"
