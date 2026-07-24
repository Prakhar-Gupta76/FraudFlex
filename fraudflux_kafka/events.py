"""Event metadata assembly and JSON serialization."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Mapping, Optional
from uuid import uuid4

from fraudflux_validation import TransactionEvent, validate_transaction_event


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _new_identifier() -> str:
    return str(uuid4())


class TransactionEventFactory:
    """Build a complete validated event from a transaction payload."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] = _utc_now,
        identifier_factory: Callable[[], str] = _new_identifier,
    ) -> None:
        self._clock = clock
        self._identifier_factory = identifier_factory

    def create(
        self,
        transaction: Mapping[str, Any],
        *,
        event_time: Optional[datetime] = None,
    ) -> TransactionEvent:
        created_at = event_time or self._clock()
        if created_at.tzinfo is None:
            raise ValueError("event_time must include timezone information")

        transaction_payload: Dict[str, Any] = deepcopy(dict(transaction))
        transaction_payload.setdefault(
            "transaction_id", f"TXN-{self._identifier_factory()}"
        )
        transaction_payload.setdefault(
            "transaction_time", created_at.isoformat()
        )
        event_payload = {
            "event_id": f"EVT-{self._identifier_factory()}",
            "event_type": "transaction.created",
            "schema_version": "1.0",
            "event_time": created_at.isoformat(),
            "transaction": transaction_payload,
        }
        return validate_transaction_event(event_payload)


def serialize_transaction_event(event: TransactionEvent) -> bytes:
    """Serialize a validated event to deterministic compact UTF-8 JSON."""
    payload = event.model_dump(mode="json")
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")

