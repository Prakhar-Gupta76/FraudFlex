"""Low-latency anomaly inference with robust deviation explanations."""

from __future__ import annotations

import time
from typing import Callable, Optional

import numpy as np
import sklearn

from fraudflux_worker import AnomalyEvaluation, FeatureSet

from .artifact import AnomalyModelArtifact, load_artifact
from .features import (
    FEATURE_SCHEMA_VERSION,
    MODEL_FEATURES,
    FeatureVectorizer,
    ModelFeature,
)


class IsolationForestAnomalyModel:
    def __init__(
        self,
        artifact: AnomalyModelArtifact,
        *,
        maximum_deviations: int = 3,
        deviation_threshold: float = 3.0,
        timer_ns: Callable[[], int] = time.perf_counter_ns,
    ) -> None:
        if artifact.feature_schema_version != FEATURE_SCHEMA_VERSION:
            raise ValueError(
                "model feature schema is incompatible with this application"
            )
        expected_names = tuple(feature.name for feature in MODEL_FEATURES)
        expected_transforms = tuple(
            feature.transform for feature in MODEL_FEATURES
        )
        if (
            artifact.feature_names != expected_names
            or artifact.feature_transforms != expected_transforms
        ):
            raise ValueError(
                "model artifact feature definitions are incompatible"
            )
        if artifact.sklearn_version != sklearn.__version__:
            raise ValueError(
                "model artifact requires scikit-learn "
                f"{artifact.sklearn_version}; running {sklearn.__version__}"
            )
        if maximum_deviations < 1:
            raise ValueError("maximum_deviations must be positive")
        if deviation_threshold < 0:
            raise ValueError("deviation_threshold cannot be negative")
        self.artifact = artifact
        self.maximum_deviations = maximum_deviations
        self.deviation_threshold = deviation_threshold
        self.timer_ns = timer_ns
        self.vectorizer = FeatureVectorizer(
            tuple(
                ModelFeature(name, transform)
                for name, transform in zip(
                    artifact.feature_names,
                    artifact.feature_transforms,
                )
            )
        )
        if self.vectorizer.names != artifact.feature_names:
            raise ValueError("artifact feature order is invalid")

    @classmethod
    def from_path(
        cls,
        path: str,
        **kwargs: object,
    ) -> "IsolationForestAnomalyModel":
        return cls(load_artifact(path), **kwargs)

    def evaluate(self, features: FeatureSet) -> AnomalyEvaluation:
        started = self.timer_ns()
        vector = self.vectorizer.vectorize(features.values)
        raw_score = -float(
            self.artifact.estimator.decision_function(
                vector.reshape(1, -1)
            )[0]
        )
        contribution = self.artifact.calibration.contribution(raw_score)
        deviations = self._deviations(vector, contribution)
        elapsed_ms = max(0.0, (self.timer_ns() - started) / 1_000_000)
        return AnomalyEvaluation(
            contribution=contribution,
            raw_score=round(raw_score, 8),
            deviations=deviations,
            model_version=self.artifact.model_version,
            inference_time_ms=round(elapsed_ms, 3),
        )

    def _deviations(
        self,
        vector: np.ndarray,
        contribution: int,
    ) -> tuple[str, ...]:
        centers = np.asarray(self.artifact.centers)
        scales = np.asarray(self.artifact.scales)
        robust_z = np.abs(vector - centers) / scales
        ranked = list(np.argsort(-robust_z))
        selected = [
            index
            for index in ranked
            if robust_z[index] >= self.deviation_threshold
        ][: self.maximum_deviations]
        if contribution >= 6 and not selected:
            selected = [
                index
                for index in ranked
                if robust_z[index] > 0
            ][: self.maximum_deviations]

        explanations = []
        for index in selected:
            direction = (
                "above" if vector[index] >= centers[index] else "below"
            )
            explanations.append(
                f"{self.artifact.feature_names[index]}: {direction} "
                f"normal (z={robust_z[index]:.2f})"
            )
        return tuple(explanations)
