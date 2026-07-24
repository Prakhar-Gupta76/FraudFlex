from __future__ import annotations

import copy
import unittest
from typing import Any, Optional

from fastapi.testclient import TestClient

from fraudflux_api import create_app
from fraudflux_storage import AnalystReviewRecord, ReviewOutcome
from tests.test_scoring_worker import build_worker, valid_event


NOW = "2026-07-24T10:00:00+00:00"


def transaction_summary() -> dict[str, Any]:
    return {
        "transaction_id": "TXN-1",
        "customer_id": "CUS-1",
        "amount_minor": 2500,
        "currency": "INR",
        "merchant_id": "MER-1",
        "transaction_time": NOW,
        "processing_status": "scored",
        "final_score": 32,
        "category": "low",
        "recommended_action": "approve",
        "processed_at": NOW,
    }


def transaction_detail() -> dict[str, Any]:
    return {
        **transaction_summary(),
        "event_id": "EVT-1",
        "account_id": "ACC-1",
        "merchant_category": "grocery",
        "device_id": "DEV-1",
        "region": "Delhi",
        "country": "IN",
        "score_category": "low",
        "override_applied": False,
        "explanation": ["Observed facts."],
        "rules_contribution": 20,
        "rule_hits": [
            {"rule_id": "R-1", "points": 20, "reason": "Observed fact."}
        ],
        "anomaly_contribution": 12,
        "anomaly_level": "moderately_unusual",
        "anomaly_deviations": ["amount_ratio"],
        "ruleset_version": "rules-v1",
        "model_version": "model-v1",
        "score_policy_version": "score-v1",
        "decision_policy_version": "decision-v1",
        "processing_latency_ms": 3.2,
    }


def alert_summary() -> dict[str, Any]:
    return {
        "alert_id": "ALERT-1",
        "transaction_id": "TXN-2",
        "customer_id": "CUS-2",
        "status": "open",
        "assigned_to": None,
        "created_at": NOW,
        "updated_at": NOW,
        "final_score": 75,
        "category": "high",
        "recommended_action": "hold_for_review",
    }


def alert_detail() -> dict[str, Any]:
    return {
        **alert_summary(),
        "assigned_at": None,
        "score_category": "high",
        "override_applied": False,
        "explanation": ["Observed facts."],
        "rules_contribution": 50,
        "rule_hits": [],
        "anomaly_contribution": 25,
        "anomaly_level": "highly_unusual",
        "anomaly_deviations": ["amount_ratio"],
        "ruleset_version": "rules-v1",
        "model_version": "model-v1",
        "score_policy_version": "score-v1",
        "decision_policy_version": "decision-v1",
        "processing_latency_ms": 4.1,
        "review_history": [],
    }


class FakeQueries:
    def __init__(self) -> None:
        self.transaction = transaction_detail()
        self.alert = alert_detail()
        self.healthy = True
        self.transaction_filters: Any = None
        self.alert_filters: Any = None

    def list_transactions(
        self,
        *,
        limit: int,
        offset: int,
        category: Optional[str] = None,
        search: Optional[str] = None,
        customer_id: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        self.transaction_filters = (
            limit,
            offset,
            category,
            search,
            customer_id,
        )
        return [transaction_summary()]

    def get_transaction(self, transaction_id: str) -> Any:
        return self.transaction if transaction_id == "TXN-1" else None

    def list_alerts(
        self,
        *,
        limit: int,
        offset: int,
        status: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        self.alert_filters = (limit, offset, status)
        return [alert_summary()]

    def get_alert(self, alert_id: str) -> Any:
        return self.alert if alert_id == "ALERT-1" else None

    def dashboard_summary(self) -> dict[str, Any]:
        return {
            "total_transactions": 10,
            "low_risk": 7,
            "medium_risk": 2,
            "high_risk": 1,
            "average_risk_score": 24.5,
            "median_processing_latency_ms": 3.1,
            "p95_processing_latency_ms": 8.2,
            "open_alerts": 2,
            "assigned_alerts": 1,
            "resolved_alerts": 3,
        }

    def health(self) -> bool:
        return self.healthy


class FakeAlerts:
    def __init__(self) -> None:
        self.reviews: list[dict[str, Any]] = []
        self.result = True
        self.missing = False

    def review(
        self,
        alert_id: str,
        *,
        review_id: str,
        analyst_id: str,
        outcome: ReviewOutcome,
        notes: Optional[str] = None,
    ) -> Optional[AnalystReviewRecord]:
        if self.missing:
            raise KeyError(alert_id)
        self.reviews.append(
            {
                "alert_id": alert_id,
                "review_id": review_id,
                "analyst_id": analyst_id,
                "outcome": outcome,
                "notes": notes,
            }
        )
        if not self.result:
            return None
        status = (
            "resolved"
            if outcome
            in {
                ReviewOutcome.CONFIRMED_FRAUD,
                ReviewOutcome.LEGITIMATE,
            }
            else "open"
        )
        return AnalystReviewRecord(
            review_id=review_id,
            alert_id=alert_id,
            analyst_id=analyst_id,
            outcome=outcome,
            notes=notes,
            previous_status="open",
            new_status=status,
            reviewed_at=NOW,
        )


class FastApiServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        worker, _, self.publisher, _, _ = build_worker()
        self.queries = FakeQueries()
        self.alerts = FakeAlerts()
        self.app = create_app(
            processor=worker.processor,
            queries=self.queries,
            alerts=self.alerts,
        )
        self.client = TestClient(self.app)

    def test_all_component_routes_are_registered(self) -> None:
        routes = {
            (method, route.path)
            for route in self.app.routes
            for method in getattr(route, "methods", ())
        }
        for expected in (
            ("POST", "/transactions/score"),
            ("GET", "/transactions"),
            ("GET", "/transactions/{transaction_id}"),
            ("GET", "/alerts"),
            ("GET", "/alerts/{alert_id}"),
            ("POST", "/alerts/{alert_id}/review"),
            ("GET", "/dashboard/summary"),
            ("GET", "/events/stream"),
            ("GET", "/health"),
            ("GET", "/metrics"),
        ):
            self.assertIn(expected, routes)

    def test_manual_scoring_uses_shared_processor_and_is_idempotent(self) -> None:
        event = valid_event()

        first = self.client.post("/transactions/score", json=event)
        second = self.client.post(
            "/transactions/score",
            json=copy.deepcopy(event),
        )

        self.assertEqual(first.status_code, 200)
        self.assertTrue(first.json()["created"])
        self.assertEqual(first.json()["final_score"], 32)
        self.assertEqual(first.json()["category"], "low")
        self.assertFalse(second.json()["created"])
        self.assertEqual(len(self.publisher.messages), 1)

    def test_invalid_manual_transaction_is_rejected_with_422(self) -> None:
        event = valid_event()
        event["transaction"]["amount_minor"] = 0

        response = self.client.post("/transactions/score", json=event)

        self.assertEqual(response.status_code, 422)

    def test_transaction_queries_filter_and_report_not_found(self) -> None:
        listed = self.client.get(
            "/transactions?limit=25&offset=5&category=high"
            "&search=CUS&customer_id=CUS-1"
        )
        found = self.client.get("/transactions/TXN-1")
        missing = self.client.get("/transactions/unknown")

        self.assertEqual(listed.status_code, 200)
        self.assertEqual(
            self.queries.transaction_filters,
            (25, 5, "high", "CUS", "CUS-1"),
        )
        self.assertEqual(found.json()["transaction_id"], "TXN-1")
        self.assertEqual(missing.status_code, 404)

    def test_interim_review_keeps_alert_active(self) -> None:
        response = self.client.post(
            "/alerts/ALERT-1/review",
            json={
                "review_id": "REVIEW-interim",
                "analyst_id": "analyst-1",
                "outcome": "needs_further_investigation",
                "notes": "Waiting for customer confirmation.",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "open")
        self.assertEqual(
            response.json()["previous_status"],
            "open",
        )

    def test_alert_queries_filter_and_include_explanations(self) -> None:
        listed = self.client.get("/alerts?status=open")
        detail = self.client.get("/alerts/ALERT-1")

        self.assertEqual(listed.status_code, 200)
        self.assertEqual(self.queries.alert_filters, (50, 0, "open"))
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["explanation"], ["Observed facts."])
        self.assertEqual(detail.json()["ruleset_version"], "rules-v1")

    def test_analyst_review_success_conflict_and_missing(self) -> None:
        payload = {
            "review_id": "REVIEW-1",
            "analyst_id": "analyst-1",
            "outcome": "confirmed_fraud",
            "notes": "Customer denied this payment.",
        }
        success = self.client.post("/alerts/ALERT-1/review", json=payload)
        self.alerts.result = False
        conflict = self.client.post("/alerts/ALERT-1/review", json=payload)
        self.alerts.missing = True
        missing = self.client.post("/alerts/unknown/review", json=payload)

        self.assertEqual(success.status_code, 200)
        self.assertEqual(success.json()["status"], "resolved")
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(missing.status_code, 404)

    def test_dashboard_and_health_report_operational_state(self) -> None:
        summary = self.client.get("/dashboard/summary")
        healthy = self.client.get("/health")
        self.queries.healthy = False
        degraded = self.client.get("/health")

        self.assertEqual(summary.json()["total_transactions"], 10)
        self.assertEqual(
            summary.json()["median_processing_latency_ms"],
            3.1,
        )
        self.assertEqual(healthy.json()["status"], "healthy")
        self.assertEqual(degraded.json()["status"], "degraded")
        self.assertEqual(
            degraded.json()["checks"]["database"],
            "unhealthy",
        )

    def test_query_limits_and_filters_are_validated(self) -> None:
        self.assertEqual(
            self.client.get("/transactions?limit=201").status_code,
            422,
        )
        self.assertEqual(
            self.client.get("/alerts?status=invalid").status_code,
            422,
        )

    def test_local_dashboard_origin_is_allowed_by_cors(self) -> None:
        response = self.client.options(
            "/dashboard/summary",
            headers={
                "Origin": "http://localhost:5173",
                "Access-Control-Request-Method": "GET",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers["access-control-allow-origin"],
            "http://localhost:5173",
        )


if __name__ == "__main__":
    unittest.main()
