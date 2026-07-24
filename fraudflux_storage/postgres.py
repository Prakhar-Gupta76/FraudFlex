"""Atomic PostgreSQL repositories for decisions, alerts, and audit history."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Mapping, Optional, Protocol, Sequence

from fraudflux_validation import validate_transaction_event
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


class Cursor(Protocol):
    def execute(self, query: str, parameters: Any = None) -> Any: ...
    def fetchone(self) -> Optional[Mapping[str, Any]]: ...
    def fetchall(self) -> Sequence[Mapping[str, Any]]: ...
    def __enter__(self) -> "Cursor": ...
    def __exit__(self, *args: Any) -> None: ...


class Connection(Protocol):
    def cursor(self) -> Cursor: ...
    def __enter__(self) -> "Connection": ...
    def __exit__(self, *args: Any) -> None: ...


@dataclass(frozen=True)
class PostgresStorageSettings:
    dsn: str = (
        "postgresql://fraudflux:fraudflux@localhost:5432/fraudflux"
    )

    def __post_init__(self) -> None:
        if not self.dsn.strip():
            raise ValueError("dsn cannot be blank")


def create_connection_factory(
    settings: Optional[PostgresStorageSettings] = None,
) -> Callable[[], Connection]:
    resolved = settings or PostgresStorageSettings()
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise RuntimeError(
            "psycopg is required for PostgreSQL storage"
        ) from exc

    def connect() -> Connection:
        return psycopg.connect(resolved.dsn, row_factory=dict_row)

    return connect


class PostgresProcessingStore:
    """ProcessingStore implementation using one transaction per mutation."""

    def __init__(self, connection_factory: Callable[[], Connection]) -> None:
        self.connection_factory = connection_factory

    def get_decision(self, event_id: str) -> Optional[StoredDecision]:
        with self.connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    _GET_DECISION,
                    {"input_event_id": event_id},
                )
                row = cursor.fetchone()
        return _stored_decision(row) if row else None

    def save_decision_if_absent(
        self,
        decision: StoredDecision,
        outbox: Sequence[OutboxMessage],
    ) -> bool:
        _validate_outbox(decision.record_id, outbox)
        event = validate_transaction_event(decision.transaction_payload)
        if event.event_id != decision.input_event_id:
            raise ValueError("stored event ID does not match transaction payload")
        if event.transaction.transaction_id != decision.transaction_id:
            raise ValueError(
                "stored transaction ID does not match transaction payload"
            )
        parameters = _decision_parameters(decision)

        with self.connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    _UPSERT_CUSTOMER_SHELL,
                    {
                        "customer_id": decision.customer_id,
                        "last_activity_at": event.transaction.transaction_time,
                    },
                )
                cursor.execute(
                    _INSERT_TRANSACTION,
                    _transaction_parameters(decision, event),
                )
                cursor.execute(_INSERT_DECISION, parameters)
                if cursor.fetchone() is None:
                    return False
                _insert_outbox(cursor, outbox)
                if decision.decision.category in {
                    RiskCategory.MEDIUM,
                    RiskCategory.HIGH,
                }:
                    cursor.execute(
                        _INSERT_ALERT,
                        {
                            "alert_id": f"ALERT-{decision.input_event_id}",
                            "record_id": decision.record_id,
                            "customer_id": decision.customer_id,
                            "transaction_id": decision.transaction_id,
                        },
                    )
                cursor.execute(
                    _INSERT_AUDIT,
                    {
                        "entity_type": "decision",
                        "entity_id": decision.record_id,
                        "action": "decision.created",
                        "actor": "fraud-scoring-worker",
                        "details": _json(
                            {
                                "event_id": decision.input_event_id,
                                "final_score": (
                                    decision.combined_score.final_score
                                ),
                                "category": (
                                    decision.decision.category.value
                                ),
                            }
                        ),
                    },
                )
        return True

    def save_rejection_if_absent(
        self,
        record_id: str,
        outbox: Sequence[OutboxMessage],
    ) -> bool:
        _validate_outbox(record_id, outbox)
        with self.connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    _INSERT_REJECTION,
                    {"record_id": record_id},
                )
                if cursor.fetchone() is None:
                    return False
                _insert_outbox(cursor, outbox)
                cursor.execute(
                    _INSERT_AUDIT,
                    {
                        "entity_type": "rejected_event",
                        "entity_id": record_id,
                        "action": "event.rejected",
                        "actor": "fraud-scoring-worker",
                        "details": _json({}),
                    },
                )
        return True

    def pending_outbox(self, record_id: str) -> Sequence[OutboxMessage]:
        with self.connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    _PENDING_OUTBOX,
                    {"record_id": record_id},
                )
                rows = cursor.fetchall()
        return tuple(
            OutboxMessage(
                outbox_id=row["outbox_id"],
                record_id=row["record_id"],
                topic=row["topic"],
                key=row["message_key"],
                payload=_json_value(row["payload"]),
            )
            for row in rows
        )

    def mark_outbox_published(self, outbox_id: str) -> None:
        with self.connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    _MARK_OUTBOX_PUBLISHED,
                    {"outbox_id": outbox_id},
                )
                if cursor.fetchone() is None:
                    raise KeyError(f"unknown outbox event {outbox_id}")


@dataclass(frozen=True)
class CustomerProfile:
    customer_id: str
    home_country: Optional[str] = None
    home_region: Optional[str] = None
    usual_countries: tuple[str, ...] = ()
    usual_regions: tuple[str, ...] = ()
    normal_behavior: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.customer_id.strip():
            raise ValueError("customer_id cannot be blank")


class PostgresCustomerProfileRepository:
    def __init__(self, connection_factory: Callable[[], Connection]) -> None:
        self.connection_factory = connection_factory

    def upsert(self, profile: CustomerProfile, *, actor: str) -> None:
        if not actor.strip():
            raise ValueError("actor cannot be blank")
        with self.connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    _UPSERT_CUSTOMER_PROFILE,
                    {
                        "customer_id": profile.customer_id,
                        "home_country": profile.home_country,
                        "home_region": profile.home_region,
                        "usual_countries": list(profile.usual_countries),
                        "usual_regions": list(profile.usual_regions),
                        "normal_behavior": _json(profile.normal_behavior),
                    },
                )
                cursor.execute(
                    _INSERT_AUDIT,
                    {
                        "entity_type": "customer_profile",
                        "entity_id": profile.customer_id,
                        "action": "customer_profile.upserted",
                        "actor": actor,
                        "details": _json({}),
                    },
                )

    def get(self, customer_id: str) -> Optional[CustomerProfile]:
        with self.connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    _GET_CUSTOMER_PROFILE,
                    {"customer_id": customer_id},
                )
                row = cursor.fetchone()
        if row is None:
            return None
        return CustomerProfile(
            customer_id=row["customer_id"],
            home_country=row["home_country"],
            home_region=row["home_region"],
            usual_countries=tuple(row["usual_countries"] or ()),
            usual_regions=tuple(row["usual_regions"] or ()),
            normal_behavior=_json_value(row["normal_behavior"]),
        )


class PostgresVersionRepository:
    def __init__(self, connection_factory: Callable[[], Connection]) -> None:
        self.connection_factory = connection_factory

    def register_ruleset(
        self,
        *,
        version: str,
        content_sha256: str,
        configuration: Mapping[str, Any],
        actor: str,
    ) -> bool:
        _validate_version_registration(version, content_sha256, actor)
        return self._register(
            _INSERT_RULESET_VERSION,
            {
                "version": version,
                "content_sha256": content_sha256.lower(),
                "configuration": _json(configuration),
                "registered_by": actor,
            },
            entity_type="ruleset_version",
            version=version,
            actor=actor,
        )

    def register_model(
        self,
        *,
        version: str,
        algorithm: str,
        artifact_sha256: str,
        metadata: Mapping[str, Any],
        actor: str,
    ) -> bool:
        _validate_version_registration(version, artifact_sha256, actor)
        if not algorithm.strip():
            raise ValueError("algorithm cannot be blank")
        return self._register(
            _INSERT_MODEL_VERSION,
            {
                "version": version,
                "algorithm": algorithm,
                "artifact_sha256": artifact_sha256.lower(),
                "metadata": _json(metadata),
                "registered_by": actor,
            },
            entity_type="model_version",
            version=version,
            actor=actor,
        )

    def _register(
        self,
        query: str,
        parameters: Mapping[str, Any],
        *,
        entity_type: str,
        version: str,
        actor: str,
    ) -> bool:
        with self.connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, parameters)
                if cursor.fetchone() is None:
                    return False
                cursor.execute(
                    _INSERT_AUDIT,
                    {
                        "entity_type": entity_type,
                        "entity_id": version,
                        "action": f"{entity_type}.registered",
                        "actor": actor,
                        "details": _json({}),
                    },
                )
        return True


class ReviewOutcome(str, Enum):
    CONFIRMED_FRAUD = "confirmed_fraud"
    LEGITIMATE = "legitimate"
    NEEDS_MORE_INFORMATION = "needs_more_information"


@dataclass(frozen=True)
class AlertRecord:
    alert_id: str
    decision_record_id: str
    customer_id: str
    transaction_id: str
    status: str
    assigned_to: Optional[str]


class PostgresAlertRepository:
    def __init__(self, connection_factory: Callable[[], Connection]) -> None:
        self.connection_factory = connection_factory

    def get(self, alert_id: str) -> Optional[AlertRecord]:
        with self.connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(_GET_ALERT, {"alert_id": alert_id})
                row = cursor.fetchone()
        return _alert(row) if row else None

    def assign(
        self,
        alert_id: str,
        *,
        analyst_id: str,
        actor: str,
    ) -> AlertRecord:
        if not analyst_id.strip() or not actor.strip():
            raise ValueError("analyst_id and actor cannot be blank")
        with self.connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    _ASSIGN_ALERT,
                    {
                        "alert_id": alert_id,
                        "analyst_id": analyst_id,
                    },
                )
                row = cursor.fetchone()
                if row is None:
                    raise KeyError(
                        f"alert {alert_id} does not exist or is resolved"
                    )
                cursor.execute(
                    _INSERT_AUDIT,
                    {
                        "entity_type": "alert",
                        "entity_id": alert_id,
                        "action": "alert.assigned",
                        "actor": actor,
                        "details": _json({"analyst_id": analyst_id}),
                    },
                )
        return _alert(row)

    def review(
        self,
        alert_id: str,
        *,
        review_id: str,
        analyst_id: str,
        outcome: ReviewOutcome,
        notes: Optional[str] = None,
    ) -> bool:
        if not review_id.strip() or not analyst_id.strip():
            raise ValueError("review_id and analyst_id cannot be blank")
        with self.connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    _LOCK_ALERT,
                    {"alert_id": alert_id},
                )
                if cursor.fetchone() is None:
                    raise KeyError(f"alert {alert_id} does not exist")
                cursor.execute(
                    _INSERT_REVIEW,
                    {
                        "review_id": review_id,
                        "alert_id": alert_id,
                        "analyst_id": analyst_id,
                        "outcome": outcome.value,
                        "notes": notes,
                    },
                )
                if cursor.fetchone() is None:
                    return False
                cursor.execute(
                    _RESOLVE_ALERT,
                    {"alert_id": alert_id},
                )
                if cursor.fetchone() is None:
                    raise KeyError(f"alert {alert_id} does not exist")
                cursor.execute(
                    _INSERT_AUDIT,
                    {
                        "entity_type": "alert",
                        "entity_id": alert_id,
                        "action": "alert.reviewed",
                        "actor": analyst_id,
                        "details": _json(
                            {
                                "review_id": review_id,
                                "outcome": outcome.value,
                                "notes": notes,
                            }
                        ),
                    },
                )
        return True


def _decision_parameters(decision: StoredDecision) -> Mapping[str, Any]:
    return {
        "record_id": decision.record_id,
        "input_event_id": decision.input_event_id,
        "transaction_id": decision.transaction_id,
        "customer_id": decision.customer_id,
        "transaction_payload": _json(decision.transaction_payload),
        "feature_values": _json(decision.feature_values),
        "rules_contribution": decision.rules.contribution,
        "rule_hits": _json(
            [
                {
                    "rule_id": hit.rule_id,
                    "points": hit.points,
                    "reason": hit.reason,
                }
                for hit in decision.rules.hits
            ]
        ),
        "ruleset_version": decision.rules.ruleset_version,
        "rule_override_action": _optional_action(
            decision.rules.override_action
        ),
        "anomaly_contribution": decision.anomaly.contribution,
        "anomaly_raw_score": decision.anomaly.raw_score,
        "anomaly_deviations": _json(decision.anomaly.deviations),
        "anomaly_level": decision.anomaly.level,
        "anomaly_inference_time_ms": (
            decision.anomaly.inference_time_ms
        ),
        "model_version": decision.anomaly.model_version,
        "uncapped_score": decision.combined_score.uncapped_score,
        "final_score": decision.combined_score.final_score,
        "score_policy_version": decision.combined_score.policy_version,
        "score_override_action": _optional_action(
            decision.combined_score.override_action
        ),
        "score_category": decision.decision.score_category.value,
        "effective_category": decision.decision.category.value,
        "recommended_action": decision.decision.action.value,
        "override_applied": decision.decision.override_applied,
        "explanation": _json(decision.decision.explanation),
        "decision_policy_version": (
            decision.decision.decision_policy_version
        ),
        "processing_latency_ms": (
            decision.decision.processing_latency_ms
        ),
        "processed_at": decision.processed_at,
    }


def _transaction_parameters(
    decision: StoredDecision,
    event: Any,
) -> Mapping[str, Any]:
    transaction = event.transaction
    return {
        "event_id": event.event_id,
        "transaction_id": transaction.transaction_id,
        "customer_id": transaction.customer_id,
        "account_id": transaction.account_id,
        "amount_minor": transaction.amount_minor,
        "currency": transaction.currency,
        "merchant_id": transaction.merchant.merchant_id,
        "merchant_category": transaction.merchant.category,
        "device_id": transaction.device.device_id,
        "region": transaction.location.city,
        "country": transaction.location.country,
        "latitude": transaction.location.latitude,
        "longitude": transaction.location.longitude,
        "authentication_result": transaction.authentication.result,
        "failed_attempts_last_10m": (
            transaction.authentication.failed_attempts_last_10m
        ),
        "transaction_time": transaction.transaction_time,
        "raw_event": _json(decision.transaction_payload),
        "processed_at": decision.processed_at,
    }


def _stored_decision(row: Mapping[str, Any]) -> StoredDecision:
    rule_hits = tuple(
        RuleHit(item["rule_id"], item["points"], item["reason"])
        for item in _json_value(row["rule_hits"])
    )
    rules = RuleEvaluation(
        contribution=row["rules_contribution"],
        hits=rule_hits,
        ruleset_version=row["ruleset_version"],
        override_action=_action_value(row["rule_override_action"]),
    )
    anomaly = AnomalyEvaluation(
        contribution=row["anomaly_contribution"],
        raw_score=row["anomaly_raw_score"],
        deviations=tuple(_json_value(row["anomaly_deviations"])),
        model_version=row["model_version"],
        inference_time_ms=row["anomaly_inference_time_ms"],
    )
    combined = CombinedRiskScore(
        rules_contribution=row["rules_contribution"],
        anomaly_contribution=row["anomaly_contribution"],
        uncapped_score=row["uncapped_score"],
        final_score=row["final_score"],
        policy_version=row["score_policy_version"],
        override_action=_action_value(row["score_override_action"]),
    )
    decision = RiskDecision(
        final_score=row["final_score"],
        score_category=RiskCategory(row["score_category"]),
        category=RiskCategory(row["effective_category"]),
        action=RecommendedAction(row["recommended_action"]),
        explanation=tuple(_json_value(row["explanation"])),
        decision_policy_version=row["decision_policy_version"],
        processing_latency_ms=row["processing_latency_ms"],
        override_applied=row["override_applied"],
    )
    return StoredDecision(
        record_id=row["record_id"],
        input_event_id=row["input_event_id"],
        transaction_id=row["transaction_id"],
        customer_id=row["customer_id"],
        transaction_payload=_json_value(row["transaction_payload"]),
        feature_values=_json_value(row["feature_values"]),
        rules=rules,
        anomaly=anomaly,
        combined_score=combined,
        decision=decision,
        processed_at=str(row["processed_at"]),
    )


def _insert_outbox(
    cursor: Cursor,
    outbox: Sequence[OutboxMessage],
) -> None:
    for message in outbox:
        cursor.execute(
            _INSERT_OUTBOX,
            {
                "outbox_id": message.outbox_id,
                "record_id": message.record_id,
                "topic": message.topic,
                "message_key": message.key,
                "payload": _json(message.payload),
            },
        )


def _validate_outbox(
    record_id: str,
    outbox: Sequence[OutboxMessage],
) -> None:
    identifiers = set()
    topics = set()
    for message in outbox:
        if message.record_id != record_id:
            raise ValueError("outbox record_id does not match its record")
        if message.outbox_id in identifiers:
            raise ValueError("outbox IDs must be unique")
        if message.topic in topics:
            raise ValueError("outbox topics must be unique for one record")
        identifiers.add(message.outbox_id)
        topics.add(message.topic)


def _validate_version_registration(
    version: str,
    sha256: str,
    actor: str,
) -> None:
    if not version.strip() or not actor.strip():
        raise ValueError("version and actor cannot be blank")
    normalized = sha256.lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError("SHA-256 digest must contain 64 hexadecimal characters")


def _alert(row: Mapping[str, Any]) -> AlertRecord:
    return AlertRecord(
        alert_id=row["alert_id"],
        decision_record_id=row["decision_record_id"],
        customer_id=row["customer_id"],
        transaction_id=row["transaction_id"],
        status=row["status"],
        assigned_to=row["assigned_to"],
    )


def _json(value: Any) -> str:
    return json.dumps(
        value,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    )


def _json_value(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _optional_action(
    action: Optional[RecommendedAction],
) -> Optional[str]:
    return action.value if action else None


def _action_value(value: Optional[str]) -> Optional[RecommendedAction]:
    return RecommendedAction(value) if value else None


_GET_DECISION = "SELECT * FROM risk_decisions WHERE input_event_id = %(input_event_id)s"

_UPSERT_CUSTOMER_SHELL = """
INSERT INTO customer_profiles (customer_id, last_activity_at)
VALUES (%(customer_id)s, %(last_activity_at)s)
ON CONFLICT (customer_id) DO UPDATE
SET last_activity_at = CASE
    WHEN customer_profiles.last_activity_at IS NULL
        THEN EXCLUDED.last_activity_at
    ELSE GREATEST(
        customer_profiles.last_activity_at,
        EXCLUDED.last_activity_at
    )
END
"""

_INSERT_TRANSACTION = """
INSERT INTO transaction_history (
    event_id, transaction_id, customer_id, account_id, amount_minor,
    currency, merchant_id, merchant_category, device_id, region, country,
    latitude, longitude, authentication_result, failed_attempts_last_10m,
    transaction_time, raw_event, processing_status, processed_at
) VALUES (
    %(event_id)s, %(transaction_id)s, %(customer_id)s, %(account_id)s,
    %(amount_minor)s, %(currency)s, %(merchant_id)s, %(merchant_category)s,
    %(device_id)s, %(region)s, %(country)s, %(latitude)s, %(longitude)s,
    %(authentication_result)s, %(failed_attempts_last_10m)s,
    %(transaction_time)s, %(raw_event)s::jsonb, 'scored', %(processed_at)s
)
ON CONFLICT (event_id) DO NOTHING
"""

_INSERT_DECISION = """
INSERT INTO risk_decisions (
    record_id, input_event_id, transaction_id, customer_id,
    transaction_payload, feature_values, rules_contribution, rule_hits,
    ruleset_version, rule_override_action, anomaly_contribution,
    anomaly_raw_score, anomaly_deviations, anomaly_level,
    anomaly_inference_time_ms, model_version, uncapped_score, final_score,
    score_policy_version, score_override_action, score_category,
    effective_category, recommended_action, override_applied, explanation,
    decision_policy_version, processing_latency_ms, processed_at
) VALUES (
    %(record_id)s, %(input_event_id)s, %(transaction_id)s, %(customer_id)s,
    %(transaction_payload)s::jsonb, %(feature_values)s::jsonb,
    %(rules_contribution)s, %(rule_hits)s::jsonb, %(ruleset_version)s,
    %(rule_override_action)s, %(anomaly_contribution)s,
    %(anomaly_raw_score)s, %(anomaly_deviations)s::jsonb, %(anomaly_level)s,
    %(anomaly_inference_time_ms)s, %(model_version)s, %(uncapped_score)s,
    %(final_score)s, %(score_policy_version)s, %(score_override_action)s,
    %(score_category)s, %(effective_category)s, %(recommended_action)s,
    %(override_applied)s, %(explanation)s::jsonb,
    %(decision_policy_version)s, %(processing_latency_ms)s, %(processed_at)s
)
ON CONFLICT (input_event_id) DO NOTHING
RETURNING record_id
"""

_INSERT_ALERT = """
INSERT INTO fraud_alerts (
    alert_id, decision_record_id, customer_id, transaction_id
) VALUES (
    %(alert_id)s, %(record_id)s, %(customer_id)s, %(transaction_id)s
)
ON CONFLICT (decision_record_id) DO NOTHING
"""

_INSERT_OUTBOX = """
INSERT INTO outbox_events (
    outbox_id, record_id, topic, message_key, payload
) VALUES (
    %(outbox_id)s, %(record_id)s, %(topic)s, %(message_key)s,
    %(payload)s::jsonb
)
"""

_INSERT_REJECTION = """
INSERT INTO rejected_events (record_id)
VALUES (%(record_id)s)
ON CONFLICT (record_id) DO NOTHING
RETURNING record_id
"""

_PENDING_OUTBOX = """
SELECT outbox_id, record_id, topic, message_key, payload
FROM outbox_events
WHERE record_id = %(record_id)s AND published_at IS NULL
ORDER BY created_at, outbox_id
"""

_MARK_OUTBOX_PUBLISHED = """
UPDATE outbox_events
SET published_at = COALESCE(published_at, CURRENT_TIMESTAMP)
WHERE outbox_id = %(outbox_id)s
RETURNING outbox_id
"""

_INSERT_AUDIT = """
INSERT INTO audit_history (
    entity_type, entity_id, action, actor, details
) VALUES (
    %(entity_type)s, %(entity_id)s, %(action)s, %(actor)s,
    %(details)s::jsonb
)
"""

_UPSERT_CUSTOMER_PROFILE = """
INSERT INTO customer_profiles (
    customer_id, home_country, home_region, usual_countries, usual_regions,
    normal_behavior
) VALUES (
    %(customer_id)s, %(home_country)s, %(home_region)s,
    %(usual_countries)s, %(usual_regions)s, %(normal_behavior)s::jsonb
)
ON CONFLICT (customer_id) DO UPDATE SET
    home_country = EXCLUDED.home_country,
    home_region = EXCLUDED.home_region,
    usual_countries = EXCLUDED.usual_countries,
    usual_regions = EXCLUDED.usual_regions,
    normal_behavior = EXCLUDED.normal_behavior,
    updated_at = CURRENT_TIMESTAMP
"""

_GET_CUSTOMER_PROFILE = """
SELECT customer_id, home_country, home_region, usual_countries,
       usual_regions, normal_behavior
FROM customer_profiles
WHERE customer_id = %(customer_id)s
"""

_INSERT_RULESET_VERSION = """
INSERT INTO ruleset_versions (
    version, content_sha256, configuration, registered_by
) VALUES (
    %(version)s, %(content_sha256)s, %(configuration)s::jsonb,
    %(registered_by)s
)
ON CONFLICT (version) DO NOTHING
RETURNING version
"""

_INSERT_MODEL_VERSION = """
INSERT INTO model_versions (
    version, algorithm, artifact_sha256, metadata, registered_by
) VALUES (
    %(version)s, %(algorithm)s, %(artifact_sha256)s,
    %(metadata)s::jsonb, %(registered_by)s
)
ON CONFLICT (version) DO NOTHING
RETURNING version
"""

_GET_ALERT = """
SELECT alert_id, decision_record_id, customer_id, transaction_id, status,
       assigned_to
FROM fraud_alerts
WHERE alert_id = %(alert_id)s
"""

_ASSIGN_ALERT = """
UPDATE fraud_alerts
SET status = 'assigned', assigned_to = %(analyst_id)s,
    assigned_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
WHERE alert_id = %(alert_id)s AND status <> 'resolved'
RETURNING alert_id, decision_record_id, customer_id, transaction_id, status,
          assigned_to
"""

_LOCK_ALERT = """
SELECT alert_id
FROM fraud_alerts
WHERE alert_id = %(alert_id)s
FOR UPDATE
"""

_INSERT_REVIEW = """
INSERT INTO analyst_reviews (
    review_id, alert_id, analyst_id, outcome, notes
) VALUES (
    %(review_id)s, %(alert_id)s, %(analyst_id)s, %(outcome)s, %(notes)s
)
ON CONFLICT (alert_id) DO NOTHING
RETURNING review_id
"""

_RESOLVE_ALERT = """
UPDATE fraud_alerts
SET status = 'resolved', updated_at = CURRENT_TIMESTAMP
WHERE alert_id = %(alert_id)s
RETURNING alert_id
"""
