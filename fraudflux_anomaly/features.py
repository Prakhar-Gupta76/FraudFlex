"""Stable numeric feature schema shared by training and inference."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np


class FeatureVectorError(ValueError):
    pass


@dataclass(frozen=True)
class ModelFeature:
    name: str
    transform: str = "identity"


FEATURE_SCHEMA_VERSION = "fraud-features-1.0.0"

MODEL_FEATURES: tuple[ModelFeature, ...] = (
    ModelFeature("amount_history_count", "log1p"),
    ModelFeature("amount_to_normal_ratio", "log1p"),
    ModelFeature("amount_deviation_from_normal", "log1p"),
    ModelFeature("transactions_previous_2m", "log1p"),
    ModelFeature("transactions_previous_1h", "log1p"),
    ModelFeature("recent_merchant_count_1h", "log1p"),
    ModelFeature("device_is_new"),
    ModelFeature("device_account_count", "log1p"),
    ModelFeature("device_first_seen_known"),
    ModelFeature("device_age_seconds", "log1p"),
    ModelFeature("device_deny_listed"),
    ModelFeature("previous_location_known"),
    ModelFeature("distance_from_previous_km", "log1p"),
    ModelFeature("seconds_since_previous_transaction", "log1p"),
    ModelFeature("travel_speed_kmh", "log1p"),
    ModelFeature("impossible_travel"),
    ModelFeature("unusual_country"),
    ModelFeature("unusual_region"),
    ModelFeature("merchant_category_rarity"),
    ModelFeature("merchant_is_new"),
    ModelFeature("merchant_fraud_rate"),
    ModelFeature("recent_authentication_failures_10m", "log1p"),
    ModelFeature("authentication_failures_then_success"),
)


class FeatureVectorizer:
    def __init__(
        self,
        features: Sequence[ModelFeature] = MODEL_FEATURES,
    ) -> None:
        if not features:
            raise ValueError("model feature schema cannot be empty")
        names = [feature.name for feature in features]
        if len(names) != len(set(names)):
            raise ValueError("model feature names must be unique")
        invalid = [
            feature.transform
            for feature in features
            if feature.transform not in {"identity", "log1p"}
        ]
        if invalid:
            raise ValueError(f"unsupported feature transform: {invalid[0]}")
        self.features = tuple(features)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(feature.name for feature in self.features)

    def vectorize(self, values: Mapping[str, Any]) -> np.ndarray:
        vector = [
            self._value(feature, values)
            for feature in self.features
        ]
        return np.asarray(vector, dtype=np.float64)

    def matrix(
        self,
        records: Sequence[Mapping[str, Any]],
    ) -> np.ndarray:
        if not records:
            raise FeatureVectorError("feature records cannot be empty")
        return np.vstack([self.vectorize(record) for record in records])

    @staticmethod
    def _value(
        feature: ModelFeature,
        values: Mapping[str, Any],
    ) -> float:
        if feature.name not in values:
            raise FeatureVectorError(
                f"required model feature {feature.name!r} is missing"
            )
        raw = values[feature.name]
        if isinstance(raw, bool):
            value = float(raw)
        else:
            try:
                value = float(raw)
            except (TypeError, ValueError) as exc:
                raise FeatureVectorError(
                    f"model feature {feature.name!r} must be numeric"
                ) from exc
        if not math.isfinite(value):
            raise FeatureVectorError(
                f"model feature {feature.name!r} must be finite"
            )
        if feature.transform == "log1p":
            if value < 0:
                raise FeatureVectorError(
                    f"model feature {feature.name!r} cannot be negative"
                )
            value = math.log1p(value)
        return value
