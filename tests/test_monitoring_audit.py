from __future__ import annotations

import unittest

from fastapi.testclient import TestClient

from fraudflux_api import create_app
from fraudflux_monitoring import (
    DecisionAuditSnapshot,
    MetricsRegistry,
    OperationalMonitor,
)
from tests.test_fastapi_service import FakeAlerts, FakeQueries
from tests.test_scoring_worker import FakeMessage, build_worker, valid_event


class MetricsRegistryTests(unittest.TestCase):
    def test_counters_gauges_distributions_and_prometheus_output(self) -> None:
        registry = MetricsRegistry(sample_limit=3)
        registry.increment(
            "fraudflux_events_produced_total",
            labels={"topic": "transactions.raw"},
        )
        registry.set_gauge(
            "fraudflux_kafka_consumer_lag",
            7,
            labels={"partition": "0"},
        )
        for value in (1, 2, 3, 100):
            registry.observe("fraudflux_scoring_latency_ms", value)

        snapshot = registry.snapshot()
        distribution = next(iter(snapshot.distributions.values()))
        rendered = registry.prometheus_text()

        self.assertEqual(distribution.count, 4)
        self.assertEqual(distribution.total, 106)
        self.assertEqual(distribution.minimum, 1)
        self.assertEqual(distribution.maximum, 100)
        self.assertEqual(distribution.p50, 3)
        self.assertEqual(distribution.p95, 100)
        self.assertIn("fraudflux_events_produced_total", rendered)
        self.assertIn('topic="transactions.raw"', rendered)
        self.assertIn('quantile="0.95"', rendered)

    def test_invalid_or_unbounded_values_are_rejected(self) -> None:
        registry = MetricsRegistry()

        with self.assertRaises(ValueError):
            registry.increment("bad metric")
        with self.assertRaises(ValueError):
            registry.observe("latency_ms", float("inf"))
        with self.assertRaises(ValueError):
            registry.increment("counter_total", -1)


class OperationalMonitorTests(unittest.TestCase):
    def test_lag_throughput_failures_and_rule_frequency(self) -> None:
        clock = iter((10.0, 12.0, 12.0)).__next__
        monitor = OperationalMonitor(monotonic=clock)
        monitor.record_event_produced("transactions.raw")
        monitor.record_event_consumed("transactions.raw", "processed")
        monitor.record_dead_letter()
        monitor.record_database_error("save_decision")
        monitor.record_model_failure("isolation-forest-v1")
        monitor.record_scoring(4.5, rule_ids=("R-1", "R-1", "R-2"))

        lag = monitor.record_consumer_lag(
            topic="transactions.raw",
            partition=0,
            high_watermark=25,
            committed_offset=20,
            consumer_group="fraudflux-scoring-worker",
        )
        snapshot = monitor.snapshot()

        self.assertEqual(lag, 5)
        self.assertEqual(
            monitor.registry.counter_value(
                "fraudflux_rule_triggers_total",
                labels={"rule_id": "R-1"},
            ),
            1,
        )
        self.assertTrue(
            any(
                name == "fraudflux_events_consumed_per_second"
                for name, _ in snapshot.gauges
            )
        )

    def test_invalid_kafka_offsets_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            OperationalMonitor().record_consumer_lag(
                topic="transactions.raw",
                partition=-1,
                high_watermark=1,
                committed_offset=0,
                consumer_group="worker",
            )


class MonitoringIntegrationTests(unittest.TestCase):
    def test_worker_records_pipeline_and_event_metrics(self) -> None:
        worker, _, _, _, store = build_worker()
        event = valid_event()

        worker.process_message(FakeMessage(event))
        snapshot = worker.monitor.snapshot()
        audit = DecisionAuditSnapshot.from_stored(
            store.get_decision(event["event_id"])
        )

        self.assertEqual(
            worker.monitor.registry.counter_value(
                "fraudflux_events_consumed_total",
                labels={
                    "topic": "transactions.raw",
                    "outcome": "processed",
                },
            ),
            1,
        )
        self.assertEqual(
            worker.monitor.registry.counter_value(
                "fraudflux_events_produced_total",
                labels={"topic": "transactions.scored"},
            ),
            1,
        )
        self.assertTrue(
            any(
                name == "fraudflux_scoring_latency_ms"
                for name, _ in snapshot.distributions
            )
        )
        self.assertEqual(audit.event_id, event["event_id"])
        self.assertEqual(audit.ruleset_version, "rules-v1")
        self.assertEqual(audit.model_version, "model-v1")
        self.assertEqual(audit.triggered_reasons, ("Amount is unusual",))

    def test_worker_records_dead_letter_once(self) -> None:
        worker, _, _, _, _ = build_worker()
        event = valid_event()
        event["transaction"]["amount_minor"] = 0
        message = FakeMessage(event)

        worker.process_message(message)
        worker.process_message(message)

        self.assertEqual(
            worker.monitor.registry.counter_value(
                "fraudflux_dead_letter_events_total",
                labels={"reason": "validation_failed"},
            ),
            1,
        )

    def test_worker_records_anomaly_model_failure(self) -> None:
        worker, _, _, _, _ = build_worker()

        class FailingModel:
            model_version = "broken-v1"

            def evaluate(self, features):
                raise RuntimeError("model unavailable")

        worker.pipeline.anomaly_model = FailingModel()

        with self.assertRaisesRegex(RuntimeError, "model unavailable"):
            worker.process_message(FakeMessage(valid_event()))

        self.assertEqual(
            worker.monitor.registry.counter_value(
                "fraudflux_model_failures_total",
                labels={"model_version": "broken-v1"},
            ),
            1,
        )

    def test_api_exposes_metrics_and_health_check(self) -> None:
        worker, _, _, _, _ = build_worker()
        monitor = OperationalMonitor()
        app = create_app(
            processor=worker.processor,
            queries=FakeQueries(),
            alerts=FakeAlerts(),
            monitor=monitor,
        )
        client = TestClient(app)

        health = client.get("/health")
        client.get("/dashboard/summary")
        metrics = client.get("/metrics")

        self.assertEqual(health.json()["checks"]["monitoring"], "healthy")
        self.assertEqual(metrics.status_code, 200)
        self.assertIn("fraudflux_api_requests_total", metrics.text)
        self.assertIn('route="/health"', metrics.text)

    def test_api_records_database_query_failures(self) -> None:
        worker, _, _, _, _ = build_worker()
        monitor = OperationalMonitor()
        queries = FakeQueries()

        def fail_query(**kwargs):
            raise RuntimeError("database unavailable")

        queries.list_transactions = fail_query
        client = TestClient(
            create_app(
                processor=worker.processor,
                queries=queries,
                alerts=FakeAlerts(),
                monitor=monitor,
            )
        )

        response = client.get("/transactions")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            monitor.registry.counter_value(
                "fraudflux_database_errors_total",
                labels={"operation": "list_transactions"},
            ),
            1,
        )


if __name__ == "__main__":
    unittest.main()
