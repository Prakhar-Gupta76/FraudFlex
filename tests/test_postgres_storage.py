from __future__ import annotations

import json
import unittest
from pathlib import Path
from typing import Any, Mapping, Optional

from fraudflux_simulator import TransactionSimulator
from fraudflux_storage import (
    CustomerProfile,
    PostgresAlertRepository,
    PostgresCustomerProfileRepository,
    PostgresProcessingStore,
    PostgresQueryRepository,
    PostgresVersionRepository,
    ReviewOutcome,
)
from fraudflux_storage.postgres import _decision_parameters
from fraudflux_worker import (
    AnomalyEvaluation,
    CombinedRiskScore,
    OutboxMessage,
    RecommendedAction,
    RiskCategory,
    RiskDecision,
    RuleEvaluation,
    RuleHit,
    StoredDecision,
)


ROOT = Path(__file__).resolve().parents[1]
NOW = "2026-07-24T10:00:00+00:00"


class FakeCursor:
    def __init__(
        self,
        responses: Optional[Mapping[str, list[Any]]] = None,
    ) -> None:
        self.responses = {
            marker: list(values)
            for marker, values in (responses or {}).items()
        }
        self.executions: list[tuple[str, Any]] = []
        self.current: Any = None

    def execute(self, query: str, parameters: Any = None) -> None:
        self.executions.append((query, parameters))
        self.current = None
        for marker, values in self.responses.items():
            if marker in query and values:
                self.current = values.pop(0)
                break

    def fetchone(self) -> Any:
        return self.current

    def fetchall(self) -> list[Any]:
        return list(self.current or [])

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *args: Any) -> None:
        return None


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self.fake_cursor = cursor
        self.entered = 0
        self.exited = 0

    def cursor(self) -> FakeCursor:
        return self.fake_cursor

    def __enter__(self) -> "FakeConnection":
        self.entered += 1
        return self

    def __exit__(self, *args: Any) -> None:
        self.exited += 1


def make_decision(*, high: bool = False) -> StoredDecision:
    payload = next(
        TransactionSimulator(seed=1101).generate(
            count=1,
            scenario="normal",
            rate=1,
        )
    ).public_event()
    rules_points = 50 if high else 20
    anomaly_points = 25 if high else 12
    score = rules_points + anomaly_points
    category = RiskCategory.HIGH if high else RiskCategory.LOW
    action = (
        RecommendedAction.HOLD if high else RecommendedAction.APPROVE
    )
    transaction = payload["transaction"]
    return StoredDecision(
        record_id=f"decision:{payload['event_id']}",
        input_event_id=payload["event_id"],
        transaction_id=transaction["transaction_id"],
        customer_id=transaction["customer_id"],
        transaction_payload=payload,
        feature_values={"amount_ratio": 4.2, "new_device": True},
        rules=RuleEvaluation(
            contribution=rules_points,
            hits=(
                RuleHit("AMOUNT_UNUSUAL", rules_points, "Amount is unusual"),
            ),
            ruleset_version="rules-v1",
        ),
        anomaly=AnomalyEvaluation(
            contribution=anomaly_points,
            raw_score=-0.42,
            deviations=("amount_ratio",),
            model_version="model-v1",
            inference_time_ms=1.25,
        ),
        combined_score=CombinedRiskScore(
            rules_contribution=rules_points,
            anomaly_contribution=anomaly_points,
            uncapped_score=score,
            final_score=score,
            policy_version="score-v1",
        ),
        decision=RiskDecision(
            final_score=score,
            category=category,
            action=action,
            explanation=("Observed amount differs from customer history.",),
            decision_policy_version="decision-v1",
            processing_latency_ms=3.5,
        ),
        processed_at="2026-07-24T10:00:00+00:00",
    )


def make_outbox(decision: StoredDecision) -> tuple[OutboxMessage, ...]:
    return (
        OutboxMessage(
            outbox_id=f"outbox:{decision.input_event_id}",
            record_id=decision.record_id,
            topic="transactions.scored",
            key=decision.customer_id,
            payload={"event_id": decision.input_event_id},
        ),
    )


class PostgresProcessingStoreTests(unittest.TestCase):
    def test_saves_decision_transaction_history_outbox_and_audit(self) -> None:
        decision = make_decision()
        cursor = FakeCursor(
            {"INSERT INTO risk_decisions": [{"record_id": decision.record_id}]}
        )
        connection = FakeConnection(cursor)
        store = PostgresProcessingStore(lambda: connection)

        created = store.save_decision_if_absent(
            decision,
            make_outbox(decision),
        )

        self.assertTrue(created)
        sql = "\n".join(query for query, _ in cursor.executions)
        self.assertIn("INSERT INTO transaction_history", sql)
        self.assertIn("INSERT INTO risk_decisions", sql)
        self.assertIn("INSERT INTO outbox_events", sql)
        self.assertIn("INSERT INTO audit_history", sql)
        self.assertNotIn("INSERT INTO fraud_alerts", sql)
        audit = next(
            parameters
            for query, parameters in cursor.executions
            if "INSERT INTO audit_history" in query
        )
        details = json.loads(audit["details"])
        self.assertEqual(details["event_id"], decision.input_event_id)
        self.assertEqual(details["final_score"], 32)
        self.assertEqual(details["ruleset_version"], "rules-v1")
        self.assertEqual(details["model_version"], "model-v1")
        self.assertEqual(
            details["triggered_rules"][0]["reason"],
            "Amount is unusual",
        )
        self.assertIn("event_time", details)
        self.assertIn("processed_at", details)
        self.assertEqual(connection.entered, 1)
        self.assertEqual(connection.exited, 1)

    def test_high_decision_creates_one_alert(self) -> None:
        decision = make_decision(high=True)
        cursor = FakeCursor(
            {"INSERT INTO risk_decisions": [{"record_id": decision.record_id}]}
        )
        store = PostgresProcessingStore(
            lambda: FakeConnection(cursor)
        )

        self.assertTrue(
            store.save_decision_if_absent(decision, make_outbox(decision))
        )

        alerts = [
            parameters
            for query, parameters in cursor.executions
            if "INSERT INTO fraud_alerts" in query
        ]
        self.assertEqual(len(alerts), 1)
        self.assertEqual(
            alerts[0]["alert_id"],
            f"ALERT-{decision.input_event_id}",
        )

    def test_duplicate_decision_does_not_create_outputs_or_audit(self) -> None:
        decision = make_decision()
        cursor = FakeCursor({"INSERT INTO risk_decisions": [None]})
        store = PostgresProcessingStore(lambda: FakeConnection(cursor))

        self.assertFalse(
            store.save_decision_if_absent(decision, make_outbox(decision))
        )

        sql = "\n".join(query for query, _ in cursor.executions)
        self.assertNotIn("INSERT INTO outbox_events", sql)
        self.assertNotIn("INSERT INTO audit_history", sql)

    def test_payload_identifiers_must_match_decision(self) -> None:
        decision = make_decision()
        mismatched = StoredDecision(
            **{
                **decision.__dict__,
                "transaction_id": "TXN-mismatch",
            }
        )
        cursor = FakeCursor()
        store = PostgresProcessingStore(lambda: FakeConnection(cursor))

        with self.assertRaisesRegex(ValueError, "transaction ID"):
            store.save_decision_if_absent(
                mismatched,
                make_outbox(mismatched),
            )
        self.assertEqual(cursor.executions, [])

    def test_duplicate_outbox_topic_is_rejected_before_database_work(self) -> None:
        decision = make_decision()
        first = make_outbox(decision)[0]
        duplicate_topic = OutboxMessage(
            outbox_id="outbox:duplicate",
            record_id=decision.record_id,
            topic=first.topic,
            key=decision.customer_id,
            payload={"duplicate": True},
        )
        cursor = FakeCursor()
        store = PostgresProcessingStore(lambda: FakeConnection(cursor))

        with self.assertRaisesRegex(ValueError, "topics must be unique"):
            store.save_decision_if_absent(
                decision,
                (first, duplicate_topic),
            )
        self.assertEqual(cursor.executions, [])

    def test_decision_round_trip_restores_domain_values(self) -> None:
        decision = make_decision(high=True)
        row = dict(_decision_parameters(decision))
        for field in (
            "transaction_payload",
            "feature_values",
            "rule_hits",
            "anomaly_deviations",
            "explanation",
        ):
            row[field] = json.loads(row[field])
        cursor = FakeCursor({"SELECT * FROM risk_decisions": [row]})
        store = PostgresProcessingStore(lambda: FakeConnection(cursor))

        restored = store.get_decision(decision.input_event_id)

        self.assertEqual(restored, decision)

    def test_pending_outbox_and_publish_confirmation(self) -> None:
        decision = make_decision()
        row = {
            "outbox_id": "outbox-1",
            "record_id": decision.record_id,
            "topic": "transactions.scored",
            "message_key": decision.customer_id,
            "payload": {"event_id": decision.input_event_id},
        }
        cursor = FakeCursor(
            {
                "FROM outbox_events": [[row]],
                "UPDATE outbox_events": [{"outbox_id": "outbox-1"}],
            }
        )
        store = PostgresProcessingStore(lambda: FakeConnection(cursor))

        pending = store.pending_outbox(decision.record_id)
        store.mark_outbox_published("outbox-1")

        self.assertEqual(pending[0].payload, row["payload"])


class PostgresOperationalRepositoryTests(unittest.TestCase):
    def test_customer_profile_is_upserted_with_audit_history(self) -> None:
        cursor = FakeCursor()
        repository = PostgresCustomerProfileRepository(
            lambda: FakeConnection(cursor)
        )
        profile = CustomerProfile(
            customer_id="CUS-1",
            home_country="IN",
            usual_countries=("IN",),
            normal_behavior={"median_amount_minor": 2500},
        )

        repository.upsert(profile, actor="profile-loader")

        self.assertEqual(len(cursor.executions), 2)
        self.assertIn("customer_profile.upserted", str(cursor.executions[1]))

    def test_alert_assignment_and_review_are_audited(self) -> None:
        alert_row = {
            "alert_id": "ALERT-1",
            "decision_record_id": "decision:1",
            "customer_id": "CUS-1",
            "transaction_id": "TXN-1",
            "status": "assigned",
            "assigned_to": "analyst-1",
        }
        cursor = FakeCursor(
            {
                "SET status = 'assigned'": [alert_row],
                "FOR UPDATE": [
                    {
                        "alert_id": "ALERT-1",
                        "status": "assigned",
                        "assigned_to": "analyst-1",
                    }
                ],
                "INSERT INTO analyst_reviews": [
                    {"review_id": "REVIEW-1", "reviewed_at": NOW}
                ],
                "SET status = %(new_status)s": [{"alert_id": "ALERT-1"}],
            }
        )
        repository = PostgresAlertRepository(
            lambda: FakeConnection(cursor)
        )

        assigned = repository.assign(
            "ALERT-1",
            analyst_id="analyst-1",
            actor="team-lead",
        )
        reviewed = repository.review(
            "ALERT-1",
            review_id="REVIEW-1",
            analyst_id="analyst-1",
            outcome=ReviewOutcome.CONFIRMED_FRAUD,
            notes="Customer confirmed the payment was not theirs.",
        )

        self.assertEqual(assigned.assigned_to, "analyst-1")
        self.assertTrue(reviewed)
        self.assertEqual(reviewed.previous_status, "assigned")
        self.assertEqual(reviewed.new_status, "resolved")
        audit_actions = [
            parameters["action"]
            for query, parameters in cursor.executions
            if "INSERT INTO audit_history" in query
        ]
        self.assertEqual(
            audit_actions,
            ["alert.assigned", "alert.reviewed"],
        )
        review_audit = [
            parameters
            for query, parameters in cursor.executions
            if "INSERT INTO audit_history" in query
        ][-1]
        details = json.loads(review_audit["details"])
        self.assertEqual(details["previous_status"], "assigned")
        self.assertEqual(details["new_status"], "resolved")
        self.assertEqual(details["reviewed_at"], NOW)

    def test_interim_review_is_append_only_and_keeps_alert_active(self) -> None:
        cursor = FakeCursor(
            {
                "FOR UPDATE": [
                    {
                        "alert_id": "ALERT-1",
                        "status": "assigned",
                        "assigned_to": "analyst-1",
                    }
                ],
                "INSERT INTO analyst_reviews": [
                    {"review_id": "REVIEW-2", "reviewed_at": NOW}
                ],
                "SET status = %(new_status)s": [{"alert_id": "ALERT-1"}],
            }
        )
        repository = PostgresAlertRepository(
            lambda: FakeConnection(cursor)
        )

        review = repository.review(
            "ALERT-1",
            review_id="REVIEW-2",
            analyst_id="analyst-1",
            outcome=ReviewOutcome.NEEDS_FURTHER_INVESTIGATION,
            notes="Waiting for customer confirmation.",
        )

        self.assertIsNotNone(review)
        self.assertEqual(review.new_status, "assigned")
        insert_parameters = next(
            parameters
            for query, parameters in cursor.executions
            if "INSERT INTO analyst_reviews" in query
        )
        self.assertEqual(
            insert_parameters["outcome"],
            "needs_further_investigation",
        )
        self.assertEqual(insert_parameters["previous_status"], "assigned")
        self.assertEqual(insert_parameters["new_status"], "assigned")

    def test_resolved_alert_cannot_overwrite_review_history(self) -> None:
        cursor = FakeCursor(
            {
                "FOR UPDATE": [
                    {
                        "alert_id": "ALERT-1",
                        "status": "resolved",
                        "assigned_to": "analyst-1",
                    }
                ]
            }
        )
        repository = PostgresAlertRepository(
            lambda: FakeConnection(cursor)
        )

        review = repository.review(
            "ALERT-1",
            review_id="REVIEW-late",
            analyst_id="analyst-2",
            outcome=ReviewOutcome.LEGITIMATE,
        )

        self.assertIsNone(review)
        self.assertEqual(len(cursor.executions), 1)

    def test_model_and_ruleset_versions_are_registered_once(self) -> None:
        digest = "a" * 64
        cursor = FakeCursor(
            {
                "INSERT INTO ruleset_versions": [{"version": "rules-v1"}],
                "INSERT INTO model_versions": [{"version": "model-v1"}],
            }
        )
        repository = PostgresVersionRepository(
            lambda: FakeConnection(cursor)
        )

        rules_created = repository.register_ruleset(
            version="rules-v1",
            content_sha256=digest,
            configuration={"rules": []},
            actor="release-pipeline",
        )
        model_created = repository.register_model(
            version="model-v1",
            algorithm="IsolationForest",
            artifact_sha256="b" * 64,
            metadata={"training_rows": 500},
            actor="release-pipeline",
        )

        self.assertTrue(rules_created)
        self.assertTrue(model_created)
        self.assertEqual(
            sum(
                "INSERT INTO audit_history" in query
                for query, _ in cursor.executions
            ),
            2,
        )

    def test_review_of_unknown_alert_is_rejected_cleanly(self) -> None:
        repository = PostgresAlertRepository(
            lambda: FakeConnection(FakeCursor())
        )

        with self.assertRaisesRegex(KeyError, "does not exist"):
            repository.review(
                "ALERT-unknown",
                review_id="REVIEW-1",
                analyst_id="analyst-1",
                outcome=ReviewOutcome.LEGITIMATE,
            )

    def test_version_registry_rejects_an_invalid_digest(self) -> None:
        repository = PostgresVersionRepository(
            lambda: FakeConnection(FakeCursor())
        )

        with self.assertRaisesRegex(ValueError, "SHA-256"):
            repository.register_model(
                version="model-v1",
                algorithm="IsolationForest",
                artifact_sha256="not-a-digest",
                metadata={},
                actor="release-pipeline",
            )

    def test_query_repository_applies_filters_and_decodes_json(self) -> None:
        transaction = {
            "transaction_id": "TXN-1",
            "explanation": '["Observed fact."]',
            "rule_hits": '[{"rule_id":"R-1"}]',
        }
        cursor = FakeCursor(
            {
                "FROM transaction_history AS th": [[transaction]],
                "FROM fraud_alerts AS fa": [[]],
                "FROM risk_decisions": [
                    {"total_transactions": 1, "low_risk": 1}
                ],
                "SELECT 1 AS healthy": [{"healthy": 1}],
            }
        )
        repository = PostgresQueryRepository(
            lambda: FakeConnection(cursor)
        )

        transactions = repository.list_transactions(
            limit=25,
            offset=5,
            category="high",
        )
        alerts = repository.list_alerts(
            limit=10,
            offset=0,
            status="open",
        )
        summary = repository.dashboard_summary()

        self.assertEqual(transactions[0]["explanation"], ["Observed fact."])
        self.assertEqual(transactions[0]["rule_hits"], [{"rule_id": "R-1"}])
        self.assertEqual(alerts, ())
        self.assertEqual(summary["total_transactions"], 1)
        self.assertTrue(repository.health())
        list_parameters = cursor.executions[0][1]
        self.assertEqual(list_parameters["category"], "high")
        self.assertEqual(list_parameters["limit"], 25)
        self.assertIsNone(list_parameters["search"])


class PostgresMigrationTests(unittest.TestCase):
    def test_operational_schema_contains_required_durable_records(self) -> None:
        migration = (
            ROOT / "infra" / "postgres" / "002_operational_storage.sql"
        ).read_text(encoding="utf-8")

        for table in (
            "risk_decisions",
            "ruleset_versions",
            "model_versions",
            "fraud_alerts",
            "analyst_reviews",
            "rejected_events",
            "outbox_events",
            "audit_history",
        ):
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", migration)
        self.assertIn("input_event_id VARCHAR(64) NOT NULL UNIQUE", migration)
        self.assertIn("transaction_id VARCHAR(64) NOT NULL UNIQUE", migration)
        self.assertIn("decision_record_id VARCHAR(140) NOT NULL UNIQUE", migration)
        self.assertIn("UNIQUE (record_id, topic)", migration)
        self.assertIn("FROM pg_constraint", migration)

    def test_compose_mounts_both_postgres_schema_files(self) -> None:
        compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")

        self.assertIn("001_feature_history.sql", compose)
        self.assertIn("002_operational_storage.sql", compose)
        self.assertIn("003_analyst_review_history.sql", compose)

    def test_review_history_migration_removes_single_review_limit(self) -> None:
        migration = (
            ROOT
            / "infra"
            / "postgres"
            / "003_analyst_review_history.sql"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "DROP CONSTRAINT IF EXISTS analyst_reviews_alert_id_key",
            migration,
        )
        self.assertIn("previous_status", migration)
        self.assertIn("new_status", migration)
        self.assertIn("needs_further_investigation", migration)
        self.assertIn("analyst_reviews_alert_time_idx", migration)


if __name__ == "__main__":
    unittest.main()
