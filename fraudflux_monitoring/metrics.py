"""Thread-safe, dependency-free operational metrics."""

from __future__ import annotations

import math
import re
from collections import defaultdict, deque
from dataclasses import dataclass
from threading import RLock
from typing import Deque, Mapping


_METRIC_NAME = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
LabelKey = tuple[tuple[str, str], ...]
SeriesKey = tuple[str, LabelKey]


@dataclass(frozen=True)
class Distribution:
    count: int
    total: float
    minimum: float
    maximum: float
    p50: float
    p95: float


@dataclass(frozen=True)
class MetricsSnapshot:
    counters: Mapping[SeriesKey, float]
    gauges: Mapping[SeriesKey, float]
    distributions: Mapping[SeriesKey, Distribution]


class MetricsRegistry:
    """Keep bounded metric state for one MVP service process."""

    def __init__(self, *, sample_limit: int = 2048) -> None:
        if sample_limit <= 0:
            raise ValueError("sample_limit must be positive")
        self._sample_limit = sample_limit
        self._counters: dict[SeriesKey, float] = defaultdict(float)
        self._gauges: dict[SeriesKey, float] = {}
        self._samples: dict[SeriesKey, Deque[float]] = {}
        self._sample_counts: dict[SeriesKey, int] = defaultdict(int)
        self._sample_sums: dict[SeriesKey, float] = defaultdict(float)
        self._sample_minimums: dict[SeriesKey, float] = {}
        self._sample_maximums: dict[SeriesKey, float] = {}
        self._lock = RLock()

    def increment(
        self,
        name: str,
        amount: float = 1,
        *,
        labels: Mapping[str, str] | None = None,
    ) -> None:
        value = _finite(amount)
        if value < 0:
            raise ValueError("counter increments cannot be negative")
        key = _series_key(name, labels)
        with self._lock:
            self._counters[key] += value

    def set_gauge(
        self,
        name: str,
        value: float,
        *,
        labels: Mapping[str, str] | None = None,
    ) -> None:
        key = _series_key(name, labels)
        with self._lock:
            self._gauges[key] = _finite(value)

    def observe(
        self,
        name: str,
        value: float,
        *,
        labels: Mapping[str, str] | None = None,
    ) -> None:
        observation = _finite(value)
        if observation < 0:
            raise ValueError("metric observations cannot be negative")
        key = _series_key(name, labels)
        with self._lock:
            samples = self._samples.setdefault(
                key,
                deque(maxlen=self._sample_limit),
            )
            samples.append(observation)
            self._sample_counts[key] += 1
            self._sample_sums[key] += observation
            self._sample_minimums[key] = min(
                observation,
                self._sample_minimums.get(key, observation),
            )
            self._sample_maximums[key] = max(
                observation,
                self._sample_maximums.get(key, observation),
            )

    def counter_value(
        self,
        name: str,
        *,
        labels: Mapping[str, str] | None = None,
    ) -> float:
        key = _series_key(name, labels)
        with self._lock:
            return self._counters.get(key, 0.0)

    def snapshot(self) -> MetricsSnapshot:
        with self._lock:
            distributions = {
                key: Distribution(
                    count=self._sample_counts[key],
                    total=self._sample_sums[key],
                    minimum=self._sample_minimums[key],
                    maximum=self._sample_maximums[key],
                    p50=_percentile(tuple(samples), 0.50),
                    p95=_percentile(tuple(samples), 0.95),
                )
                for key, samples in self._samples.items()
            }
            return MetricsSnapshot(
                counters=dict(self._counters),
                gauges=dict(self._gauges),
                distributions=distributions,
            )

    def prometheus_text(self) -> str:
        snapshot = self.snapshot()
        lines: list[str] = []
        declared: set[str] = set()
        for (name, labels), value in sorted(snapshot.counters.items()):
            if name not in declared:
                lines.append(f"# TYPE {name} counter")
                declared.add(name)
            lines.append(f"{name}{_labels(labels)} {_number(value)}")
        for (name, labels), value in sorted(snapshot.gauges.items()):
            if name not in declared:
                lines.append(f"# TYPE {name} gauge")
                declared.add(name)
            lines.append(f"{name}{_labels(labels)} {_number(value)}")
        for (name, labels), distribution in sorted(
            snapshot.distributions.items()
        ):
            if name not in declared:
                lines.append(f"# TYPE {name} summary")
                declared.add(name)
            lines.append(
                f"{name}{_labels(labels, {'quantile': '0.5'})} "
                f"{_number(distribution.p50)}"
            )
            lines.append(
                f"{name}{_labels(labels, {'quantile': '0.95'})} "
                f"{_number(distribution.p95)}"
            )
            lines.append(
                f"{name}_count{_labels(labels)} {distribution.count}"
            )
            lines.append(
                f"{name}_sum{_labels(labels)} "
                f"{_number(distribution.total)}"
            )
        return "\n".join(lines) + ("\n" if lines else "")


def _series_key(
    name: str,
    labels: Mapping[str, str] | None,
) -> SeriesKey:
    if not _METRIC_NAME.fullmatch(name):
        raise ValueError(f"invalid metric name {name!r}")
    normalized: list[tuple[str, str]] = []
    for key, value in (labels or {}).items():
        if not _METRIC_NAME.fullmatch(key):
            raise ValueError(f"invalid metric label {key!r}")
        normalized.append((key, str(value)))
    return name, tuple(sorted(normalized))


def _finite(value: float) -> float:
    resolved = float(value)
    if not math.isfinite(resolved):
        raise ValueError("metric values must be finite")
    return resolved


def _percentile(values: tuple[float, ...], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


def _labels(
    labels: LabelKey,
    extra: Mapping[str, str] | None = None,
) -> str:
    combined = dict(labels)
    combined.update(extra or {})
    if not combined:
        return ""
    rendered = ",".join(
        f'{key}="{_escape(value)}"'
        for key, value in sorted(combined.items())
    )
    return "{" + rendered + "}"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _number(value: float) -> str:
    return str(int(value)) if value.is_integer() else repr(value)
