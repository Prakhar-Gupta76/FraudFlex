"""Deterministic customer behavioural feature calculation."""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable, Mapping, Optional, Sequence

from fraudflux_validation import TransactionEvent
from fraudflux_worker import CustomerHistory, FeatureSet


@dataclass(frozen=True)
class FeatureCalculatorConfig:
    normal_lookback_days: int = 90
    recent_max_lookback_days: int = 30
    impossible_travel_minimum_km: float = 100.0
    impossible_travel_speed_kmh: float = 900.0

    def __post_init__(self) -> None:
        if self.normal_lookback_days < 1:
            raise ValueError("normal_lookback_days must be positive")
        if self.recent_max_lookback_days < 1:
            raise ValueError("recent_max_lookback_days must be positive")
        if self.impossible_travel_minimum_km < 0:
            raise ValueError("impossible_travel_minimum_km cannot be negative")
        if self.impossible_travel_speed_kmh <= 0:
            raise ValueError("impossible_travel_speed_kmh must be positive")


class CustomerFeatureCalculator:
    """Calculate point-in-time-safe values for rules and anomaly models."""

    def __init__(
        self,
        config: Optional[FeatureCalculatorConfig] = None,
    ) -> None:
        self.config = config or FeatureCalculatorConfig()

    def calculate(
        self,
        event: TransactionEvent,
        history: CustomerHistory,
    ) -> FeatureSet:
        if history.customer_id != event.transaction.customer_id:
            raise ValueError("history belongs to a different customer")

        transaction = event.transaction
        current_time = transaction.transaction_time
        records = _normalize_records(history.values.get("transactions", ()))
        prior = tuple(
            record
            for record in records
            if record["transaction_time"] < current_time
        )
        same_currency = tuple(
            record
            for record in prior
            if record["currency"] == transaction.currency
            and current_time - record["transaction_time"]
            <= timedelta(days=self.config.normal_lookback_days)
        )

        values: dict[str, Any] = {}
        values.update(self._amount_features(event, same_currency))
        values.update(self._velocity_features(event, prior))
        values.update(self._device_features(event, prior, history.values))
        values.update(self._location_features(event, prior, history.values))
        values.update(self._merchant_features(event, prior, history.values))
        values.update(self._authentication_features(event, prior))
        return FeatureSet(values)

    def _amount_features(
        self,
        event: TransactionEvent,
        records: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        current = event.transaction.amount_minor
        amounts = [int(record["amount_minor"]) for record in records]
        average = statistics.fmean(amounts) if amounts else 0.0
        median = float(statistics.median(amounts)) if amounts else 0.0
        recent_cutoff = event.transaction.transaction_time - timedelta(
            days=self.config.recent_max_lookback_days
        )
        recent_amounts = [
            int(record["amount_minor"])
            for record in records
            if record["transaction_time"] >= recent_cutoff
        ]
        normal = median or average
        ratio = current / normal if normal > 0 else 1.0
        if len(amounts) >= 2:
            standard_deviation = statistics.pstdev(amounts)
        else:
            standard_deviation = 0.0
        if standard_deviation > 0:
            deviation = abs(current - average) / standard_deviation
        elif normal > 0:
            deviation = abs(current - normal) / normal
        else:
            deviation = 0.0

        return {
            "amount_history_count": len(amounts),
            "customer_average_amount_minor": _rounded(average),
            "customer_median_amount_minor": _rounded(median),
            "recent_max_amount_minor": max(recent_amounts, default=0),
            "amount_to_normal_ratio": _rounded(ratio),
            "amount_deviation_from_normal": _rounded(deviation),
        }

    @staticmethod
    def _velocity_features(
        event: TransactionEvent,
        records: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        current_time = event.transaction.transaction_time
        previous_two_minutes = _within(records, current_time, timedelta(minutes=2))
        previous_hour = _within(records, current_time, timedelta(hours=1))
        return {
            "transactions_previous_2m": len(previous_two_minutes),
            "transactions_previous_1h": len(previous_hour),
            "amount_spent_previous_2m_minor": sum(
                int(record["amount_minor"]) for record in previous_two_minutes
            ),
            "amount_spent_previous_1h_minor": sum(
                int(record["amount_minor"]) for record in previous_hour
            ),
            "recent_merchant_count_1h": len(
                {record["merchant_id"] for record in previous_hour}
            ),
        }

    @staticmethod
    def _device_features(
        event: TransactionEvent,
        records: Sequence[Mapping[str, Any]],
        history: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        device = event.transaction.device
        known_device_ids = {record["device_id"] for record in records}
        intelligence = _mapping(history.get("device"))
        first_seen = _optional_datetime(intelligence.get("first_seen_at"))
        current_time = event.transaction.transaction_time
        age_seconds = (
            max(0.0, (current_time - first_seen).total_seconds())
            if first_seen is not None and first_seen <= current_time
            else 0.0
        )
        is_new = (
            device.trust_status == "new"
            or (
                device.device_id not in known_device_ids
                and first_seen is None
            )
        )
        deny_listed = (
            device.trust_status == "deny_listed"
            or bool(intelligence.get("deny_listed", False))
        )
        return {
            "device_is_new": is_new,
            "device_is_known": not is_new and not deny_listed,
            "device_account_count": max(
                0, int(intelligence.get("account_count", 0))
            ),
            "device_first_seen_known": first_seen is not None,
            "device_age_seconds": _rounded(age_seconds),
            "device_deny_listed": deny_listed,
        }

    def _location_features(
        self,
        event: TransactionEvent,
        records: Sequence[Mapping[str, Any]],
        history: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        current = event.transaction
        previous = max(
            records,
            key=lambda record: record["transaction_time"],
            default=None,
        )
        if previous is None:
            distance = 0.0
            elapsed_seconds = 0.0
            speed_kmh = 0.0
        else:
            distance = _haversine_km(
                float(previous["latitude"]),
                float(previous["longitude"]),
                current.location.latitude,
                current.location.longitude,
            )
            elapsed_seconds = max(
                0.0,
                (current.transaction_time - previous["transaction_time"])
                .total_seconds(),
            )
            speed_kmh = (
                distance / (elapsed_seconds / 3600)
                if elapsed_seconds > 0
                else (math.inf if distance > 0 else 0.0)
            )

        countries = {
            str(record["country"]).casefold()
            for record in records
            if record.get("country")
        }
        regions = {
            str(record["region"]).casefold()
            for record in records
            if record.get("region")
        }
        countries.update(
            str(item).casefold()
            for item in history.get("usual_countries", ())
        )
        regions.update(
            str(item).casefold()
            for item in history.get("usual_regions", ())
        )
        current_country = current.location.country.casefold()
        current_region = current.location.city.casefold()
        impossible = (
            distance >= self.config.impossible_travel_minimum_km
            and speed_kmh > self.config.impossible_travel_speed_kmh
        )
        return {
            "previous_location_known": previous is not None,
            "distance_from_previous_km": _rounded(distance),
            "seconds_since_previous_transaction": _rounded(elapsed_seconds),
            "travel_speed_kmh": (
                _rounded(speed_kmh) if math.isfinite(speed_kmh) else 1_000_000.0
            ),
            "impossible_travel": impossible,
            "unusual_country": bool(countries)
            and current_country not in countries,
            "unusual_region": bool(regions) and current_region not in regions,
        }

    @staticmethod
    def _merchant_features(
        event: TransactionEvent,
        records: Sequence[Mapping[str, Any]],
        history: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        merchant = event.transaction.merchant
        total = len(records)
        category_count = sum(
            record["merchant_category"] == merchant.category
            for record in records
        )
        merchant_ids = {record["merchant_id"] for record in records}
        intelligence = _mapping(history.get("merchant"))
        fraud_rate = float(intelligence.get("fraud_rate", 0.0))
        return {
            "merchant_category_rarity": _rounded(
                1.0 - (category_count / total) if total else 0.0
            ),
            "merchant_is_new": merchant.merchant_id not in merchant_ids,
            "merchant_fraud_rate": _rounded(
                min(1.0, max(0.0, fraud_rate))
            ),
        }

    @staticmethod
    def _authentication_features(
        event: TransactionEvent,
        records: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        current_time = event.transaction.transaction_time
        recent = _within(records, current_time, timedelta(minutes=10))
        recorded_failures = sum(
            record["authentication_result"] == "failure"
            for record in recent
        )
        declared_failures = (
            event.transaction.authentication.failed_attempts_last_10m
        )
        failures = max(recorded_failures, declared_failures)
        return {
            "recent_authentication_failures_10m": failures,
            "authentication_failures_then_success": (
                event.transaction.authentication.result == "success"
                and failures > 0
            ),
        }


def _normalize_records(raw_records: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(raw_records, Iterable) or isinstance(
        raw_records, (str, bytes, Mapping)
    ):
        raise ValueError("history transactions must be a sequence")

    normalized = []
    required = {
        "amount_minor",
        "currency",
        "merchant_id",
        "merchant_category",
        "device_id",
        "region",
        "country",
        "latitude",
        "longitude",
        "authentication_result",
        "transaction_time",
    }
    for raw in raw_records:
        record = _mapping(raw)
        missing = required.difference(record)
        if missing:
            raise ValueError(
                "history transaction is missing: "
                + ", ".join(sorted(missing))
            )
        copy = dict(record)
        copy["transaction_time"] = _datetime(record["transaction_time"])
        normalized.append(copy)
    return tuple(normalized)


def _within(
    records: Sequence[Mapping[str, Any]],
    current_time: datetime,
    window: timedelta,
) -> tuple[Mapping[str, Any], ...]:
    return tuple(
        record
        for record in records
        if timedelta(0)
        < current_time - record["transaction_time"]
        <= window
    )


def _datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError("history timestamp must be a datetime or ISO string")
    if result.tzinfo is None:
        raise ValueError("history timestamps must be timezone-aware")
    return result


def _optional_datetime(value: Any) -> Optional[datetime]:
    return None if value is None else _datetime(value)


def _mapping(value: Any) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("history values must contain mappings")
    return value


def _rounded(value: float) -> float:
    return round(float(value), 6)


def _haversine_km(
    latitude_one: float,
    longitude_one: float,
    latitude_two: float,
    longitude_two: float,
) -> float:
    radius_km = 6371.0088
    lat_one = math.radians(latitude_one)
    lat_two = math.radians(latitude_two)
    delta_lat = math.radians(latitude_two - latitude_one)
    delta_lon = math.radians(longitude_two - longitude_one)
    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat_one)
        * math.cos(lat_two)
        * math.sin(delta_lon / 2) ** 2
    )
    return radius_km * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
