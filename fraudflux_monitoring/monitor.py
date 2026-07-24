"""FraudFlux-specific operational metric vocabulary."""

from __future__ import annotations

import time
from typing import Callable, Iterable

from .metrics import MetricsRegistry, MetricsSnapshot


class OperationalMonitor:
    """Record low-cardinality metrics without changing business decisions."""

    def __init__(
        self,
        registry: MetricsRegistry | None = None,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.registry = registry or MetricsRegistry()
        self._monotonic = monotonic
        self._started_at = monotonic()

    def record_event_produced(self, topic: str) -> None:
        self.registry.increment(
            "fraudflux_events_produced_total",
            labels={"topic": topic},
        )

    def record_event_consumed(self, topic: str, outcome: str) -> None:
        self.registry.increment(
            "fraudflux_events_consumed_total",
            labels={"topic": topic, "outcome": outcome},
        )

    def record_dead_letter(self, reason: str = "validation_failed") -> None:
        self.registry.increment(
            "fraudflux_dead_letter_events_total",
            labels={"reason": reason},
        )

    def record_scoring(
        self,
        latency_ms: float,
        *,
        rule_ids: Iterable[str] = (),
    ) -> None:
        self.registry.observe(
            "fraudflux_scoring_latency_ms",
            latency_ms,
        )
        self.registry.increment("fraudflux_transactions_scored_total")
        for rule_id in set(rule_ids):
            self.registry.increment(
                "fraudflux_rule_triggers_total",
                labels={"rule_id": rule_id},
            )

    def record_api_request(
        self,
        method: str,
        route: str,
        status_code: int,
        latency_ms: float,
    ) -> None:
        labels = {
            "method": method.upper(),
            "route": route,
            "status": str(status_code),
        }
        self.registry.increment(
            "fraudflux_api_requests_total",
            labels=labels,
        )
        self.registry.observe(
            "fraudflux_api_latency_ms",
            latency_ms,
            labels=labels,
        )

    def record_database_error(self, operation: str) -> None:
        self.registry.increment(
            "fraudflux_database_errors_total",
            labels={"operation": operation},
        )

    def record_model_failure(self, model_version: str = "unknown") -> None:
        self.registry.increment(
            "fraudflux_model_failures_total",
            labels={"model_version": model_version},
        )

    def record_publish_failure(self, topic: str) -> None:
        self.registry.increment(
            "fraudflux_event_publish_failures_total",
            labels={"topic": topic},
        )

    def record_consumer_lag(
        self,
        *,
        topic: str,
        partition: int,
        high_watermark: int,
        committed_offset: int,
        consumer_group: str,
    ) -> int:
        if partition < 0 or high_watermark < 0 or committed_offset < 0:
            raise ValueError("Kafka offsets and partition must be non-negative")
        lag = max(0, high_watermark - committed_offset)
        self.registry.set_gauge(
            "fraudflux_kafka_consumer_lag",
            lag,
            labels={
                "topic": topic,
                "partition": str(partition),
                "consumer_group": consumer_group,
            },
        )
        return lag

    def snapshot(self) -> MetricsSnapshot:
        self._refresh_process_metrics()
        return self.registry.snapshot()

    def prometheus_text(self) -> str:
        self._refresh_process_metrics()
        return self.registry.prometheus_text()

    def _refresh_process_metrics(self) -> None:
        uptime = max(0.0, self._monotonic() - self._started_at)
        self.registry.set_gauge("fraudflux_process_uptime_seconds", uptime)
        denominator = uptime if uptime > 0 else 1.0
        produced = sum(
            value
            for (name, _), value in self.registry.snapshot().counters.items()
            if name == "fraudflux_events_produced_total"
        )
        consumed = sum(
            value
            for (name, _), value in self.registry.snapshot().counters.items()
            if name == "fraudflux_events_consumed_total"
        )
        self.registry.set_gauge(
            "fraudflux_events_produced_per_second",
            produced / denominator,
        )
        self.registry.set_gauge(
            "fraudflux_events_consumed_per_second",
            consumed / denominator,
        )
