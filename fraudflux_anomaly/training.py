"""Isolation Forest model training and calibration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional

import numpy as np
import sklearn
from sklearn.ensemble import IsolationForest

from fraudflux_worker import FeatureSet

from .artifact import ARTIFACT_FORMAT_VERSION, AnomalyModelArtifact
from .features import (
    FEATURE_SCHEMA_VERSION,
    MODEL_FEATURES,
    FeatureVectorizer,
)
from .normalization import ScoreCalibration


@dataclass(frozen=True)
class TrainerConfig:
    n_estimators: int = 100
    max_samples: int | str = "auto"
    contamination: float | str = "auto"
    random_state: int = 42
    min_training_samples: int = 50

    def __post_init__(self) -> None:
        if self.n_estimators < 10:
            raise ValueError("n_estimators must be at least 10")
        if self.min_training_samples < 2:
            raise ValueError("min_training_samples must be at least 2")
        if isinstance(self.max_samples, int) and self.max_samples < 2:
            raise ValueError("integer max_samples must be at least 2")
        if self.max_samples != "auto" and not isinstance(
            self.max_samples, int
        ):
            raise ValueError("max_samples must be 'auto' or an integer")
        if self.contamination != "auto":
            if (
                not isinstance(self.contamination, (int, float))
                or isinstance(self.contamination, bool)
                or not 0 < float(self.contamination) <= 0.5
            ):
                raise ValueError(
                    "contamination must be 'auto' or between 0 and 0.5"
                )


class IsolationForestTrainer:
    def __init__(
        self,
        config: Optional[TrainerConfig] = None,
        *,
        vectorizer: Optional[FeatureVectorizer] = None,
    ) -> None:
        self.config = config or TrainerConfig()
        self.vectorizer = vectorizer or FeatureVectorizer()
        if self.vectorizer.features != MODEL_FEATURES:
            raise ValueError(
                "trainer feature schema does not match "
                f"{FEATURE_SCHEMA_VERSION}"
            )

    def train(
        self,
        records: Iterable[FeatureSet | Mapping[str, Any]],
        *,
        model_version: str,
        trained_at: Optional[datetime] = None,
    ) -> AnomalyModelArtifact:
        if not model_version.strip():
            raise ValueError("model_version cannot be blank")
        normalized = [
            record.values if isinstance(record, FeatureSet) else record
            for record in records
        ]
        if len(normalized) < self.config.min_training_samples:
            raise ValueError(
                "insufficient normal training records: "
                f"received {len(normalized)}, "
                f"require {self.config.min_training_samples}"
            )
        matrix = self.vectorizer.matrix(normalized)
        estimator = IsolationForest(
            n_estimators=self.config.n_estimators,
            max_samples=self.config.max_samples,
            contamination=self.config.contamination,
            random_state=self.config.random_state,
            n_jobs=1,
        )
        estimator.fit(matrix)
        raw_scores = -estimator.decision_function(matrix)
        calibration = ScoreCalibration.from_scores(raw_scores)
        centers = np.median(matrix, axis=0)
        absolute_deviation = np.abs(matrix - centers)
        robust_scales = np.median(absolute_deviation, axis=0) * 1.4826
        standard_deviation = np.std(matrix, axis=0)
        scales = np.where(
            robust_scales > 1e-12,
            robust_scales,
            np.where(standard_deviation > 1e-12, standard_deviation, 1.0),
        )
        timestamp = trained_at or datetime.now(timezone.utc)
        if timestamp.tzinfo is None:
            raise ValueError("trained_at must be timezone-aware")
        return AnomalyModelArtifact(
            artifact_format_version=ARTIFACT_FORMAT_VERSION,
            model_version=model_version.strip(),
            feature_schema_version=FEATURE_SCHEMA_VERSION,
            feature_names=self.vectorizer.names,
            feature_transforms=tuple(
                feature.transform
                for feature in self.vectorizer.features
            ),
            estimator=estimator,
            calibration=calibration,
            centers=tuple(float(value) for value in centers),
            scales=tuple(float(value) for value in scales),
            trained_at=timestamp.isoformat(),
            training_samples=len(normalized),
            sklearn_version=sklearn.__version__,
        )
