"""Versioned model artifact persistence."""

from __future__ import annotations

import math
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import joblib

from .normalization import ScoreCalibration


ARTIFACT_FORMAT_VERSION = 1


@dataclass(frozen=True)
class AnomalyModelArtifact:
    artifact_format_version: int
    model_version: str
    feature_schema_version: str
    feature_names: tuple[str, ...]
    feature_transforms: tuple[str, ...]
    estimator: Any
    calibration: ScoreCalibration
    centers: tuple[float, ...]
    scales: tuple[float, ...]
    trained_at: str
    training_samples: int
    sklearn_version: str

    def __post_init__(self) -> None:
        if self.artifact_format_version != ARTIFACT_FORMAT_VERSION:
            raise ValueError("unsupported anomaly artifact format")
        if not self.model_version.strip():
            raise ValueError("model_version cannot be blank")
        if not self.feature_schema_version.strip():
            raise ValueError("feature_schema_version cannot be blank")
        if not self.feature_names:
            raise ValueError("artifact feature_names cannot be empty")
        if len(self.feature_names) != len(set(self.feature_names)):
            raise ValueError("artifact feature_names must be unique")
        if len(self.feature_transforms) != len(self.feature_names):
            raise ValueError(
                "artifact transforms do not match feature schema"
            )
        if any(
            transform not in {"identity", "log1p"}
            for transform in self.feature_transforms
        ):
            raise ValueError("artifact contains an unsupported transform")
        if (
            len(self.centers) != len(self.feature_names)
            or len(self.scales) != len(self.feature_names)
        ):
            raise ValueError("artifact statistics do not match feature schema")
        if any(not math.isfinite(value) for value in self.centers):
            raise ValueError("artifact centers must be finite")
        if any(
            not math.isfinite(value) or value <= 0
            for value in self.scales
        ):
            raise ValueError("artifact scales must be finite and positive")
        if self.training_samples < 1:
            raise ValueError("training_samples must be positive")
        try:
            trained_at = datetime.fromisoformat(
                self.trained_at.replace("Z", "+00:00")
            )
        except (AttributeError, ValueError) as exc:
            raise ValueError("trained_at must be an ISO datetime") from exc
        if trained_at.tzinfo is None:
            raise ValueError("trained_at must be timezone-aware")
        if not self.sklearn_version.strip():
            raise ValueError("sklearn_version cannot be blank")
        if not isinstance(self.calibration, ScoreCalibration):
            raise ValueError("artifact calibration is invalid")
        if not callable(getattr(self.estimator, "decision_function", None)):
            raise ValueError("artifact estimator cannot perform inference")


def save_artifact(
    artifact: AnomalyModelArtifact,
    path: str | Path,
) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        joblib.dump(artifact, temporary)
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()


def load_artifact(path: str | Path) -> AnomalyModelArtifact:
    """Load a trusted local artifact; joblib files must not be untrusted."""
    source = Path(path)
    try:
        artifact = joblib.load(source)
    except OSError as exc:
        raise ValueError(f"cannot read anomaly artifact {source}: {exc}") from exc
    if not isinstance(artifact, AnomalyModelArtifact):
        raise ValueError("file does not contain a FraudFlux anomaly artifact")
    artifact.__post_init__()
    return artifact
