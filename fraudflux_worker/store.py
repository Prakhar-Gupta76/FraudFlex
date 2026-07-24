"""Thread-safe in-memory implementation of the worker persistence contract."""

from __future__ import annotations

from threading import RLock
from typing import Dict, Optional, Sequence, Set, Tuple

from .domain import OutboxMessage, StoredDecision


class InMemoryProcessingStore:
    """Test/development store; PostgreSQL will replace it in Component 11."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._decisions: Dict[str, StoredDecision] = {}
        self._rejections: Set[str] = set()
        self._outbox_by_record: Dict[str, Tuple[OutboxMessage, ...]] = {}
        self._published_outbox_ids: Set[str] = set()

    def get_decision(self, event_id: str) -> Optional[StoredDecision]:
        with self._lock:
            return self._decisions.get(event_id)

    def save_decision_if_absent(
        self,
        decision: StoredDecision,
        outbox: Sequence[OutboxMessage],
    ) -> bool:
        with self._lock:
            if decision.input_event_id in self._decisions:
                return False
            self._validate_outbox(decision.record_id, outbox)
            self._decisions[decision.input_event_id] = decision
            self._outbox_by_record[decision.record_id] = tuple(outbox)
            return True

    def save_rejection_if_absent(
        self,
        record_id: str,
        outbox: Sequence[OutboxMessage],
    ) -> bool:
        with self._lock:
            if record_id in self._rejections:
                return False
            self._validate_outbox(record_id, outbox)
            self._rejections.add(record_id)
            self._outbox_by_record[record_id] = tuple(outbox)
            return True

    def pending_outbox(self, record_id: str) -> Sequence[OutboxMessage]:
        with self._lock:
            return tuple(
                message
                for message in self._outbox_by_record.get(record_id, ())
                if message.outbox_id not in self._published_outbox_ids
            )

    def mark_outbox_published(self, outbox_id: str) -> None:
        with self._lock:
            self._published_outbox_ids.add(outbox_id)

    @property
    def decision_count(self) -> int:
        with self._lock:
            return len(self._decisions)

    @property
    def rejection_count(self) -> int:
        with self._lock:
            return len(self._rejections)

    @staticmethod
    def _validate_outbox(
        record_id: str,
        outbox: Sequence[OutboxMessage],
    ) -> None:
        identifiers = set()
        for message in outbox:
            if message.record_id != record_id:
                raise ValueError("outbox record_id does not match its decision")
            if message.outbox_id in identifiers:
                raise ValueError("outbox IDs must be unique")
            identifiers.add(message.outbox_id)

