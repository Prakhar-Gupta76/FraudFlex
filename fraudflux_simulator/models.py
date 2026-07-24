"""Domain models for synthetic FraudFlux transaction events."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List


@dataclass(frozen=True)
class Location:
    city: str
    country: str
    latitude: float
    longitude: float


@dataclass(frozen=True)
class Merchant:
    merchant_id: str
    name: str
    category: str
    location: Location


@dataclass
class CustomerProfile:
    customer_id: str
    account_id: str
    home_location: Location
    normal_amount_minor: int
    known_devices: List[str]
    preferred_categories: List[str]
    last_customer_activity: datetime
    last_transaction_at: datetime
    last_transaction_location: Location


@dataclass(frozen=True)
class GroundTruth:
    is_fraud: bool
    scenario: str
    expected_signals: List[str]


@dataclass(frozen=True)
class GeneratedTransaction:
    event_id: str
    event_time: datetime
    transaction: Dict[str, Any]
    ground_truth: GroundTruth

    def public_event(self) -> Dict[str, Any]:
        """Return the event contract visible to the future risk pipeline."""
        return {
            "event_id": self.event_id,
            "event_type": "transaction.created",
            "schema_version": "1.0",
            "event_time": self.event_time.isoformat(),
            "transaction": self.transaction,
        }

    def evaluation_record(self) -> Dict[str, Any]:
        """Return an event plus simulator-only ground truth."""
        record = self.public_event()
        record["ground_truth"] = asdict(self.ground_truth)
        return record

