"""Empirical anomaly-score calibration to the FraudFlux 0-30 policy."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence

import numpy as np


@dataclass(frozen=True)
class ScoreCalibration:
    median: float
    p90: float
    p97: float
    p995: float
    maximum: float

    def __post_init__(self) -> None:
        anchors = (
            self.median,
            self.p90,
            self.p97,
            self.p995,
            self.maximum,
        )
        if any(not math.isfinite(value) for value in anchors):
            raise ValueError("score calibration values must be finite")
        if tuple(sorted(anchors)) != anchors:
            raise ValueError("score calibration values must be ordered")

    @classmethod
    def from_scores(
        cls,
        raw_scores: Sequence[float] | np.ndarray,
    ) -> "ScoreCalibration":
        scores = np.asarray(raw_scores, dtype=np.float64)
        if scores.ndim != 1 or scores.size == 0:
            raise ValueError("calibration requires a non-empty score vector")
        if not np.isfinite(scores).all():
            raise ValueError("calibration scores must be finite")
        anchors = np.percentile(scores, [50, 90, 97, 99.5, 100])
        return cls(*(float(value) for value in anchors))

    def contribution(self, raw_score: float) -> int:
        if not math.isfinite(raw_score):
            raise ValueError("raw anomaly score must be finite")
        if raw_score <= self.median:
            return 0
        if raw_score <= self.p90:
            return _segment(raw_score, self.median, self.p90, 1, 5)
        if raw_score <= self.p97:
            return _segment(raw_score, self.p90, self.p97, 6, 10)
        if raw_score <= self.p995:
            return _segment(raw_score, self.p97, self.p995, 11, 20)
        if raw_score <= self.maximum:
            return _segment(raw_score, self.p995, self.maximum, 21, 29)
        return 30


def _segment(
    value: float,
    lower: float,
    upper: float,
    minimum: int,
    maximum: int,
) -> int:
    if upper <= lower:
        return maximum
    fraction = (value - lower) / (upper - lower)
    scaled = minimum + fraction * (maximum - minimum)
    return max(minimum, min(maximum, int(round(scaled))))
