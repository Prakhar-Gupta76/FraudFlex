"""Environment-driven FastAPI runtime composition."""

from __future__ import annotations

import os

from fraudflux_anomaly import IsolationForestAnomalyModel
from fraudflux_decision import InitialDecisionEngine
from fraudflux_features import CustomerFeatureCalculator, PostgresHistoryProvider
from fraudflux_kafka import KafkaProducerSettings
from fraudflux_risk import InitialRiskScoreCombiner
from fraudflux_rules import YamlRulesEngine
from fraudflux_storage import (
    PostgresAlertRepository,
    PostgresProcessingStore,
    PostgresQueryRepository,
    PostgresStorageSettings,
    create_connection_factory,
)
from fraudflux_worker import (
    DecisionProcessor,
    KafkaOutputPublisher,
    SharedScoringPipeline,
)

from .application import create_app


def create_runtime_app():
    """Build the real API from environment configuration."""
    model_path = os.getenv("FRAUDFLUX_MODEL_ARTIFACT", "").strip()
    if not model_path:
        raise RuntimeError(
            "FRAUDFLUX_MODEL_ARTIFACT must point to a trained model artifact"
        )
    dsn = os.getenv(
        "FRAUDFLUX_POSTGRES_DSN",
        PostgresStorageSettings().dsn,
    )
    bootstrap_servers = os.getenv(
        "FRAUDFLUX_KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )
    connection_factory = create_connection_factory(
        PostgresStorageSettings(dsn=dsn)
    )
    pipeline = SharedScoringPipeline(
        history_provider=PostgresHistoryProvider(connection_factory),
        feature_calculator=CustomerFeatureCalculator(),
        rules_engine=YamlRulesEngine.default(),
        anomaly_model=IsolationForestAnomalyModel.from_path(model_path),
        risk_combiner=InitialRiskScoreCombiner(),
        decision_engine=InitialDecisionEngine(),
    )
    processor = DecisionProcessor(
        pipeline=pipeline,
        store=PostgresProcessingStore(connection_factory),
        publisher=KafkaOutputPublisher.from_settings(
            KafkaProducerSettings(
                bootstrap_servers=bootstrap_servers,
                client_id="fraudflux-api-output",
            )
        ),
    )
    return create_app(
        processor=processor,
        queries=PostgresQueryRepository(connection_factory),
        alerts=PostgresAlertRepository(connection_factory),
    )
